# Monthly Engineering Static Audit (2026-02-17)

_Non-destructive checks only (read commands + report artifact)._

## Repository State
```text
A  AI_AGENT_WORKFLOW.md
?? reports/
```

## Source-of-Truth Docs Presence
```text
-rw-r--r--@ 1 elliotkarikari  staff   4617 Feb 17 14:24 .env.example
-rw-r--r--@ 1 elliotkarikari  staff   8331 Feb 17 16:25 AGENTS.md
-rw-r--r--@ 1 elliotkarikari  staff  15397 Feb 17 14:24 QUICKSTART.md
-rw-r--r--@ 1 elliotkarikari  staff  46553 Feb 17 14:49 docs/ARCHITECTURE.md
-rw-r--r--@ 1 elliotkarikari  staff  19762 Feb 16 00:10 docs/LIMITATIONS.md
-rw-r--r--@ 1 elliotkarikari  staff  30651 Feb 17 17:32 docs/METHODOLOGY.md
-rw-r--r--@ 1 elliotkarikari  staff  25369 Feb 17 17:34 docs/architecture/DATA_SOURCES.md
```

## CI Workflow Presence
```text
.github/workflows/integration-smoke.yml
.github/workflows/ci.yml
```

## Migration Inventory + Prefix Check
```text
006_layer2_accessibility_overhaul.sql
007_layer1_economic_accessibility_overhaul.sql
008_layer1_economic_opportunity_index.sql
009_layer4_housing_affordability_overhaul.sql
010_layer3_education_accessibility_overhaul.sql
011_layer5_demographic_equity_overhaul.sql
012_layer6_risk_vulnerability_overhaul.sql
013_layer1_qwi_layer4_hud_additions.sql
014_layer5_low_vacancy_prediction.sql
015_layer1_prediction.sql
016_layer2_prediction.sql
017_layer3_prediction.sql
018_layer4_prediction.sql
019_layer6_prediction.sql
020_layer5_prediction.sql
021_layer3_school_directory_id_length.sql
022_layer1_accessibility_network_enhancements.sql
023_layer1_od_commute_flow_columns.sql
024_layer2_acs_flow_enrichment.sql
Migration prefixes are unique
```

## Hardcoded Year Risk Scan
```text
Year literal checks passed
```

## ETL Row-by-Row Hotspot Scan
```text
src/processing/multiyear_scoring.py:141:        result = db.execute(query, {"as_of_year": as_of_year})
src/processing/multiyear_scoring.py:330:        db.execute(delete_sql, {"as_of_year": int(as_of_year)})
src/processing/multiyear_scoring.py:358:        for _, row in df.iterrows():
src/ingest/layer3_education_accessibility.py:243:        result = db.execute(
src/ingest/layer3_education_accessibility.py:1077:        db.execute(
src/ingest/layer3_education_accessibility.py:1108:        for _, row in schools_df.iterrows():
src/ingest/layer3_education_accessibility.py:1175:        db.execute(
src/ingest/layer3_education_accessibility.py:1218:        for _, row in df.iterrows():
src/ingest/layer3_education_accessibility.py:1350:        for _, row in df.iterrows():
src/ingest/layer3_education_accessibility.py:1400:            db.execute(
src/ingest/layer5_demographics.py:683:        db.execute(
src/ingest/layer5_demographics.py:718:        for _, row in df.iterrows():
src/ingest/layer5_demographics.py:720:            db.execute(insert_sql, row_dict)
src/ingest/layer5_demographics.py:812:            year_to_pop = {row["data_year"]: row["pop_age_25_44"] for _, row in sub.iterrows()}
src/ingest/layer5_demographics.py:814:                row["data_year"]: row["households_total"] for _, row in sub.iterrows()
src/ingest/layer2_accessibility.py:1324:        db.execute(
src/ingest/layer2_accessibility.py:1363:        for _, row in df.iterrows():
src/ingest/layer2_accessibility.py:1435:        db.execute(text(ddl))
src/ingest/layer2_accessibility.py:1437:            db.execute(
src/ingest/layer2_accessibility.py:1523:        db.execute(text(ddl))
src/ingest/layer2_accessibility.py:1524:        db.execute(
src/ingest/layer2_accessibility.py:1553:        for _, row in flow_df.iterrows():
src/ingest/layer2_accessibility.py:1620:        db.execute(text(ddl))
src/ingest/layer2_accessibility.py:1621:        db.execute(
src/ingest/layer2_accessibility.py:1641:        for _, row in flow_summary.iterrows():
src/ingest/layer2_accessibility.py:1693:            existing_cols_rows = db.execute(
src/ingest/layer2_accessibility.py:1729:                db.execute(
src/ingest/layer2_accessibility.py:1837:        for _, row in df.iterrows():
src/ingest/layer2_accessibility.py:1886:            db.execute(
src/export/geojson_export.py:182:    for _, row in df.iterrows():
src/export/geojson_export.py:397:        db.execute(
src/ingest/layer6_risk_vulnerability.py:1419:        db.execute(
src/ingest/layer6_risk_vulnerability.py:1501:        for _, row in df.iterrows():
src/ingest/layer6_risk_vulnerability.py:1779:        for _, row in top_risk.iterrows():
src/processing/timeseries_features.py:154:        result = db.execute(query, {"geoid": geoid, "min_year": min_year})
src/processing/timeseries_features.py:288:        db.execute(delete_sql, {"as_of_year": as_of_year})
src/processing/timeseries_features.py:387:        result = db.execute(
src/processing/scoring.py:73:    for _, row in df.iterrows():
src/processing/scoring.py:204:        for _, row in layer_scores_df.iterrows():
src/processing/scoring.py:262:            db.execute(sql, scores)
src/processing/normalization.py:345:        db.execute(create_table_sql)
src/processing/normalization.py:351:        for _, row in merged.iterrows():
src/processing/normalization.py:374:                        db.execute(
src/ingest/layer5_demographic_equity.py:716:        for _, row in county_group.iterrows():
src/ingest/layer5_demographic_equity.py:1063:        db.execute(
src/ingest/layer5_demographic_equity.py:1110:        for _, row in df.iterrows():
src/ingest/layer5_demographic_equity.py:1266:        for _, row in df.iterrows():
src/ingest/layer5_demographic_equity.py:1318:            db.execute(
src/ingest/policy_persistence.py:258:    for _, row in all_counties.iterrows():
src/ingest/policy_persistence.py:326:            db.execute(
src/ingest/policy_persistence.py:336:        for _, row in results_df.iterrows():
src/ingest/policy_persistence.py:340:            db.execute(insert_sql if use_databricks_backend else upsert_sql, params)
src/processing/multiyear_classification.py:72:        query_result = db.execute(query, {"as_of_year": as_of_year})
src/processing/multiyear_classification.py:382:        db.execute(text(f"DELETE FROM {table_name('final_synthesis_current')}"))
src/processing/multiyear_classification.py:410:        for _, row in df.iterrows():
src/ingest/layer6_risk.py:406:        db.execute(
src/ingest/layer6_risk.py:432:        for _, row in df.iterrows():
src/ingest/layer6_risk.py:434:            db.execute(insert_sql, row_dict)
src/processing/classification.py:313:    for _, row in layer_scores_df.iterrows():
src/processing/classification.py:406:        for _, row in classifications_df.iterrows():
src/processing/classification.py:439:            db.execute(sql, params)
src/ingest/layer3_schools.py:405:        db.execute(text("DELETE FROM layer3_school_trajectory"))
src/ingest/layer3_schools.py:408:        for _, row in df.iterrows():
src/ingest/layer3_schools.py:409:            db.execute(
src/ingest/layer4_housing.py:295:        for _, row in df.iterrows():
src/ingest/layer4_housing.py:352:            db.execute(insert_sql, row_dict)
src/ingest/layer4_housing.py:438:            row["data_year"]: row["price_to_income_ratio"] for _, row in sub.iterrows()
src/ingest/layer4_housing.py:454:            row["data_year"]: row["permits_per_1000_households"] for _, row in sub.iterrows()
src/ingest/layer4_housing_affordability.py:1289:        db.execute(
src/ingest/layer4_housing_affordability.py:1339:        for _, row in df.iterrows():
src/ingest/layer4_housing_affordability.py:1440:        result = db.execute(
src/ingest/layer4_housing_affordability.py:1493:        for _, row in df.iterrows():
src/ingest/layer4_housing_affordability.py:1661:            max_year = db.execute(text(f"SELECT MAX(data_year) FROM {L4_COUNTY_TABLE}")).scalar()
src/ingest/layer1_economic_accessibility.py:503:            rows = db.execute(sql, {"od_year": int(od_year)}).fetchall()
src/ingest/layer1_economic_accessibility.py:1785:        db.execute(
src/ingest/layer1_economic_accessibility.py:1796:        for _, row in df.iterrows():
src/ingest/layer1_economic_accessibility.py:1803:            db.execute(
src/ingest/layer1_economic_accessibility.py:1883:        local_result = db.execute(
src/ingest/layer1_economic_accessibility.py:1918:        for _, row in df.iterrows():
src/ingest/layer1_economic_accessibility.py:1920:                db.execute(
src/ingest/layer1_economic_accessibility.py:1938:                db.execute(
src/ingest/layer1_economic_accessibility.py:2007:            db.execute(
```

## Docs Consistency Scan
```text
    4474 frontend/map.js
Docs consistency checks passed
```

## Documentation Update Checklist
- [ ] docs/ARCHITECTURE.md
- [ ] docs/METHODOLOGY.md
- [ ] docs/architecture/DATA_SOURCES.md
- [ ] docs/LIMITATIONS.md
- [ ] QUICKSTART.md
- [ ] AGENTS.md
- [ ] .env.example
