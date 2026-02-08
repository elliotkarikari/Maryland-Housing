from pathlib import Path


def test_databricks_schema_includes_v2_layer_tables_and_columns():
    schema_path = Path("data/schemas/databricks_schema.sql")
    schema_sql = schema_path.read_text(encoding="utf-8")

    required_snippets = [
        "CREATE TABLE IF NOT EXISTS layer1_economic_opportunity_tract",
        "CREATE TABLE IF NOT EXISTS layer2_mobility_accessibility_tract",
        "accessibility_version STRING",
        "qwi_emp_total INT",
        "capital_investment_score DECIMAL(10,6)",
        "low_vacancy_county_flag BOOLEAN",
        "vacancy_predicted BOOLEAN",
    ]

    missing = [snippet for snippet in required_snippets if snippet not in schema_sql]
    assert not missing, f"Missing required Databricks schema snippets: {missing}"
