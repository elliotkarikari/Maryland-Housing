import config.database as dbcfg


def test_table_name_postgres_passthrough(monkeypatch):
    monkeypatch.setattr(dbcfg, "DATABASE_BACKEND", "postgres", raising=False)
    assert dbcfg.table_name("layer1_lodes_od_raw") == "layer1_lodes_od_raw"


def test_table_name_databricks_routes_by_medallion(monkeypatch):
    monkeypatch.setattr(dbcfg, "DATABASE_BACKEND", "databricks", raising=False)
    monkeypatch.setattr(dbcfg.settings, "DATABRICKS_CATALOG", "maryland_atlas", raising=False)
    monkeypatch.setattr(dbcfg.settings, "DATABRICKS_BRONZE_SCHEMA", "bronze", raising=False)
    monkeypatch.setattr(dbcfg.settings, "DATABRICKS_SILVER_SCHEMA", "silver", raising=False)
    monkeypatch.setattr(dbcfg.settings, "DATABRICKS_GOLD_SCHEMA", "gold", raising=False)

    assert (
        dbcfg.table_name("layer1_lodes_od_raw") == "`maryland_atlas`.`bronze`.`layer1_lodes_od_raw`"
    )
    assert (
        dbcfg.table_name("layer3_education_accessibility_tract")
        == "`maryland_atlas`.`silver`.`layer3_education_accessibility_tract`"
    )
    assert (
        dbcfg.table_name("final_synthesis_current")
        == "`maryland_atlas`.`gold`.`final_synthesis_current`"
    )
    # Unknown/new pull tables default to bronze.
    assert (
        dbcfg.table_name("future_county_pull_raw")
        == "`maryland_atlas`.`bronze`.`future_county_pull_raw`"
    )


def test_table_name_databricks_respects_explicit_schema(monkeypatch):
    monkeypatch.setattr(dbcfg, "DATABASE_BACKEND", "databricks", raising=False)
    monkeypatch.setattr(dbcfg.settings, "DATABRICKS_CATALOG", "maryland_atlas", raising=False)

    assert (
        dbcfg.table_name("bronze.layer1_lodes_od_raw")
        == "`maryland_atlas`.`bronze`.`layer1_lodes_od_raw`"
    )
    assert (
        dbcfg.table_name("other_catalog.bronze.layer1_lodes_od_raw")
        == "`other_catalog`.`bronze`.`layer1_lodes_od_raw`"
    )
