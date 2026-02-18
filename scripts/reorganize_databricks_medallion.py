#!/usr/bin/env python3
"""Reorganize Databricks tables from a single schema into medallion schemas.

Default behavior uses `--layout layered`:
- bronze: raw landing/source tables
- silver: refined intermediate + tract tables
- gold: serving and final synthesis tables

`--layout serving-first` remains available to move everything into gold.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Dict, List
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text


BRONZE_TABLES = {
    "layer1_lodes_od_raw",
    "layer1_lodes2_od_raw",
    "layer2_acs_flows_raw",
    "layer2_gtfs_feeds_raw",
    "mobility_gtfs_feeds",
    "mobility_computation_cache",
    "education_msde_data",
    "education_schooldigger_data",
    "education_schooldigger_quarantine",
}

SILVER_TABLES = {
    "education_school_directory",
    "layer1_economic_opportunity_tract",
    "layer2_county_general_flows",
    "layer2_mobility_accessibility_tract",
    "layer3_education_accessibility_tract",
    "layer4_housing_affordability_tract",
    "layer5_demographic_equity_tract",
    "layer6_risk_vulnerability_tract",
}

GOLD_TABLES = {
    "md_counties",
    "data_refresh_log",
    "layer1_employment_gravity",
    "layer2_mobility_optionality",
    "layer3_school_trajectory",
    "layer4_housing_elasticity",
    "layer5_demographic_momentum",
    "layer6_risk_drag",
    "policy_persistence",
    "layer_scores",
    "county_classifications",
    "export_versions",
    "layer_timeseries_features",
    "layer_summary_scores",
    "final_synthesis_current",
    "normalized_features",
    "ai_document",
    "ai_extraction",
    "ai_evidence_link",
    "schema_migrations",
    "v_latest_synthesis",
    "v_timeseries_summary",
}


@dataclass(frozen=True)
class CatalogObject:
    table_name: str
    table_type: str


@dataclass(frozen=True)
class MovePlanItem:
    name: str
    kind: str
    source_schema: str
    target_schema: str


def _quote_ident(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def _fqtn(catalog: str, schema: str, name: str) -> str:
    return ".".join((_quote_ident(catalog), _quote_ident(schema), _quote_ident(name)))


def _fqs(catalog: str, schema: str) -> str:
    return ".".join((_quote_ident(catalog), _quote_ident(schema)))


def _required_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _build_databricks_url(catalog: str, schema: str) -> str:
    explicit_url = (os.getenv("DATABRICKS_SQLALCHEMY_URL") or "").strip()
    if explicit_url:
        return explicit_url

    hostname = _required_env("DATABRICKS_SERVER_HOSTNAME")
    http_path = _required_env("DATABRICKS_HTTP_PATH")
    token = _required_env("DATABRICKS_ACCESS_TOKEN")

    return (
        f"databricks://token:{quote_plus(token)}@{hostname}"
        f"?http_path={quote_plus(http_path, safe='/')}&catalog={quote_plus(catalog)}"
        f"&schema={quote_plus(schema)}"
    )


def _fetch_objects(engine, catalog: str, source_schema: str) -> List[CatalogObject]:
    query = text(
        """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog = :catalog
          AND table_schema = :schema
        ORDER BY table_name
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query, {"catalog": catalog, "schema": source_schema}).fetchall()

    return [CatalogObject(table_name=row.table_name, table_type=row.table_type) for row in rows]


def _object_exists(engine, catalog: str, schema: str, name: str) -> bool:
    query = text(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_catalog = :catalog
          AND table_schema = :schema
          AND table_name = :table_name
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(
            query,
            {"catalog": catalog, "schema": schema, "table_name": name},
        ).fetchone()
    return row is not None


def _classify_target_schema(
    object_name: str,
    layout: str,
    bronze_schema: str,
    silver_schema: str,
    gold_schema: str,
) -> str:
    if layout == "serving-first":
        return gold_schema

    if object_name in GOLD_TABLES:
        return gold_schema

    if (
        object_name in BRONZE_TABLES
        or object_name.endswith("_raw")
        or object_name.startswith("raw_")
    ):
        return bronze_schema

    if object_name in SILVER_TABLES or object_name.endswith("_tract"):
        return silver_schema

    # Unknown/new objects default to bronze so raw pulls land there first.
    return bronze_schema


def _create_schema_if_missing(conn, catalog: str, schema: str) -> None:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {_fqs(catalog, schema)}"))


def _move_object(conn, catalog: str, move: MovePlanItem) -> None:
    source = _fqtn(catalog, move.source_schema, move.name)
    target = _fqtn(catalog, move.target_schema, move.name)
    if move.kind == "VIEW":
        conn.execute(text(f"ALTER VIEW {source} RENAME TO {target}"))
    else:
        conn.execute(text(f"ALTER TABLE {source} RENAME TO {target}"))


def _create_source_view(conn, catalog: str, move: MovePlanItem) -> None:
    source = _fqtn(catalog, move.source_schema, move.name)
    target = _fqtn(catalog, move.target_schema, move.name)
    conn.execute(text(f"CREATE OR REPLACE VIEW {source} AS SELECT * FROM {target}"))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reorganize Databricks objects into bronze/silver/gold schemas"
    )
    parser.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", "maryland_atlas"))
    parser.add_argument("--source-schema", default="default")
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--silver-schema", default="silver")
    parser.add_argument("--gold-schema", default="gold")
    parser.add_argument(
        "--layout",
        choices=("serving-first", "layered"),
        default="layered",
        help="serving-first moves everything into gold; layered uses curated routing.",
    )
    parser.add_argument(
        "--create-source-views",
        action="store_true",
        help="Create read-only compatibility views in source schema after moving objects.",
    )
    parser.add_argument(
        "--skip-existing-target",
        action="store_true",
        help="Skip objects that already exist in the target schema.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes. Without this flag, the script runs in dry-run mode.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    try:
        engine = create_engine(_build_databricks_url(args.catalog, args.source_schema))
    except Exception as exc:
        print(f"ERROR: Failed to create Databricks engine: {exc}", file=sys.stderr)
        return 1

    try:
        objects = _fetch_objects(engine, args.catalog, args.source_schema)
    except Exception as exc:
        print(
            f"ERROR: Failed to list objects in {args.catalog}.{args.source_schema}: {exc}",
            file=sys.stderr,
        )
        return 1

    if not objects:
        print(f"No objects found in {args.catalog}.{args.source_schema}; nothing to do.")
        return 0

    move_plan: List[MovePlanItem] = []
    skipped_existing: List[str] = []

    for obj in objects:
        target_schema = _classify_target_schema(
            object_name=obj.table_name,
            layout=args.layout,
            bronze_schema=args.bronze_schema,
            silver_schema=args.silver_schema,
            gold_schema=args.gold_schema,
        )
        if target_schema == args.source_schema:
            continue

        kind = "VIEW" if obj.table_type.upper() == "VIEW" else "TABLE"

        if _object_exists(engine, args.catalog, target_schema, obj.table_name):
            if args.skip_existing_target:
                skipped_existing.append(f"{target_schema}.{obj.table_name}")
                continue
            print(
                "ERROR: Target already exists and --skip-existing-target was not set: "
                f"{args.catalog}.{target_schema}.{obj.table_name}",
                file=sys.stderr,
            )
            return 1

        move_plan.append(
            MovePlanItem(
                name=obj.table_name,
                kind=kind,
                source_schema=args.source_schema,
                target_schema=target_schema,
            )
        )

    if not move_plan:
        print("No objects require moving after target checks.")
        return 0

    by_schema: Dict[str, int] = {}
    for item in move_plan:
        by_schema[item.target_schema] = by_schema.get(item.target_schema, 0) + 1

    print("Planned move summary:")
    for schema_name in sorted(by_schema):
        print(f"  - {args.catalog}.{schema_name}: {by_schema[schema_name]} object(s)")
    print(f"  - total: {len(move_plan)} object(s)")

    if skipped_existing:
        print(f"  - skipped existing targets: {len(skipped_existing)}")

    for item in move_plan:
        print(
            f"    {item.kind:<5} {args.catalog}.{item.source_schema}.{item.name} "
            f"-> {args.catalog}.{item.target_schema}.{item.name}"
        )

    if not args.apply:
        print("Dry run complete. Re-run with --apply to execute.")
        return 0

    try:
        with engine.begin() as conn:
            _create_schema_if_missing(conn, args.catalog, args.bronze_schema)
            _create_schema_if_missing(conn, args.catalog, args.silver_schema)
            _create_schema_if_missing(conn, args.catalog, args.gold_schema)

            for item in move_plan:
                _move_object(conn, args.catalog, item)

            if args.create_source_views:
                for item in move_plan:
                    _create_source_view(conn, args.catalog, item)
    except Exception as exc:
        print(f"ERROR: Failed while applying medallion reorg: {exc}", file=sys.stderr)
        return 1
    finally:
        engine.dispose()

    print("Medallion reorganization completed successfully.")
    if args.create_source_views:
        print(
            "Compatibility views were created in source schema. "
            "Switch runtime writes to gold before running ingest jobs."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
