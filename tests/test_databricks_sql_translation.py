import os

os.environ.setdefault("MAPBOX_ACCESS_TOKEN", "test-token")
os.environ.setdefault("CENSUS_API_KEY", "test-census")

from config.databricks import (
    _normalize_databricks_hostname,
    _rewrite_insert_on_conflict,
    _translate_params,
    _translate_pg_to_databricks,
)


def test_translate_any_array_param_to_in_clause():
    sql = "DELETE FROM layer5_demographic_momentum WHERE data_year = ANY(:years) AND fips_code = :fips"
    translated, values = _translate_params(sql, {"years": [2023, 2024], "fips": "24031"})

    assert "data_year IN (?, ?)" in translated
    assert translated.endswith("fips_code = ?")
    assert values == [2023, 2024, "24031"]


def test_translate_list_param_to_json_string():
    sql = "INSERT INTO county_classifications (primary_strengths) VALUES (:primary_strengths)"
    translated, values = _translate_params(sql, {"primary_strengths": ["Employment", "Schools"]})

    assert translated == "INSERT INTO county_classifications (primary_strengths) VALUES (?)"
    assert values == ['["Employment", "Schools"]']


def test_translate_jsonb_cast_and_type():
    sql = "INSERT INTO layer_summary_scores (weights) VALUES (CAST(:weights AS jsonb))"
    translated = _translate_pg_to_databricks(sql)

    assert "jsonb" not in translated.lower()
    assert "CAST(" not in translated
    assert ":weights" in translated


def test_rewrite_on_conflict_do_nothing():
    sql = """
        INSERT INTO layer1_employment_gravity (fips_code, data_year)
        VALUES (:fips_code, :data_year)
        ON CONFLICT (fips_code, data_year) DO NOTHING
    """
    rewritten = _rewrite_insert_on_conflict(sql)

    assert rewritten is not None
    assert len(rewritten) == 1
    assert "WHERE NOT EXISTS" in rewritten[0]
    assert "fips_code = :fips_code" in rewritten[0]
    assert "data_year = :data_year" in rewritten[0]


def test_rewrite_on_conflict_do_update_to_delete_insert():
    sql = """
        INSERT INTO layer_scores (fips_code, data_year, composite_normalized)
        VALUES (:fips_code, :data_year, :composite_normalized)
        ON CONFLICT (fips_code, data_year)
        DO UPDATE SET composite_normalized = EXCLUDED.composite_normalized
    """
    rewritten = _rewrite_insert_on_conflict(sql)

    assert rewritten is not None
    assert len(rewritten) == 2
    assert rewritten[0].startswith("DELETE FROM layer_scores")
    assert rewritten[1].startswith("INSERT INTO layer_scores")


def test_normalize_databricks_hostname_plain_host():
    host = _normalize_databricks_hostname("adb-123.4.azuredatabricks.net")
    assert host == "adb-123.4.azuredatabricks.net"


def test_normalize_databricks_hostname_url():
    host = _normalize_databricks_hostname("https://adb-123.4.azuredatabricks.net/")
    assert host == "adb-123.4.azuredatabricks.net"
