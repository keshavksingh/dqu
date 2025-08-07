import pytest
import pandas as pd
import json
from dqu.kernel.engine.pandas_engine import PandasEngine

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": [1, 2, 2, 4],
        "name": ["Alice", "Bob", "Bob", ""],
        "age": [25, 30, 30, None],
        "score": [90, 85, 85, 70],
        "category": ["A", "B", "B", "C"],
        "date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-02", "2023-01-03"])
    })

@pytest.fixture
def engine(sample_df):
    return PandasEngine(sample_df)

def test_run_dup_check_basic(engine):
    result = engine.run_dup_check(["id"], evaluation="basic")
    result_dict = json.loads(result)
    assert result_dict["status"] == "Failed"
    assert result_dict["dqu_failed_count"] > 0

def test_run_dup_check_advanced(engine):
    result, df = engine.run_dup_check(["id"], evaluation="advanced")
    result_dict = json.loads(result)
    assert not df.empty
    assert result_dict["status"] == "Failed"

def test_run_empty_check(engine):
    result, df = engine.run_empty_check(["name", "age"], evaluation="advanced")
    result_dict = json.loads(result)
    assert result_dict["dqu_failed_count"] == 1
    assert df.iloc[0]["name"] == ""

def test_run_unique_check(engine):
    result, df = engine.run_unique_check("id", evaluation="advanced")
    result_dict = json.loads(result)
    assert result_dict["status"] == "Failed"
    assert not df.empty

def test_run_dtype_check(engine):
    result, df = engine.run_dtype_check({"age": "float"}, evaluation="advanced")
    result_dict = json.loads(result)
    assert result_dict["status"] == "Success" or result_dict["status"] == "Failed"

def test_run_stringformat_check(engine):
    result, df = engine.run_stringformat_check("name", r"^[A-Za-z]+$", evaluation="advanced")
    result_dict = json.loads(result)
    assert "dqu_failed_count" in result_dict

def test_run_schemavalidation_check(engine):
    expected_schema = {"id": "int64", "name": "object", "age": "float64", "score": "int64", "category": "object", "date": "datetime64[ns]"}
    result = engine.run_schemavalidation_check(expected_schema)
    result_dict = json.loads(result)
    assert "status" in result_dict

def test_run_range_check(engine):
    result, df = engine.run_range_check("score", 80, 100, evaluation="advanced")
    result_dict = json.loads(result)
    assert result_dict["dqu_failed_count"] == 1

def test_run_categoricalvalues_check(engine):
    result, df = engine.run_categoricalvalues_check("category", ["A", "B"], evaluation="advanced")
    result_dict = json.loads(result)
    assert result_dict["dqu_failed_count"] == 1

def test_run_statisticaldistribution_check_feature_drift(engine):
    reference_stats = {"mean": 80, "std": 10}
    result = engine.run_statisticaldistribution_check("score", "feature_drift", reference_stats, tolerance=20)
    result_dict = json.loads(result)
    assert "dqu_drift_mean" in result_dict

def test_run_statisticaldistribution_check_label_balance(engine):
    result = engine.run_statisticaldistribution_check("category", "label_balance")
    result_dict = json.loads(result)
    assert "dqu_distribution" in result_dict

def test_run_datafreshness_check(engine):
    result = engine.run_datafreshness_check("date", "2d")
    result_dict = json.loads(result)
    assert "latest_timestamp" in result_dict

def test_run_referential_integrity_check(engine):
    ref_df = pd.DataFrame({"ref_id": [1, 2, 4]})
    result, df = engine.run_referential_integrity_check("id", ref_df, "ref_id", evaluation="advanced")
    result_dict = json.loads(result)
    assert isinstance(df, pd.DataFrame)
    assert "dqu_failed_count" in result_dict

def test_run_rowcount_check(engine):
    result = engine.run_rowcount_check(min_rows=2, max_rows=10)
    result_dict = json.loads(result)
    assert result_dict["status"] == "Success"

def test_run_custom_check_column(engine):
    result, df = engine.run_custom_check("score", lambda x: x >= 80, evaluation="advanced")
    result_dict = json.loads(result)
    assert "dqu_failed_count" in result_dict

def test_run_custom_check_row(engine):
    result, df = engine.run_custom_check(None, lambda row: row["score"] >= 70, evaluation="advanced")
    result_dict = json.loads(result)
    assert "dqu_failed_count" in result_dict

def test_run_dup_check_invalid_column(engine):
    with pytest.raises(KeyError):
        engine.run_dup_check(["nonexistent_column"])

def test_run_empty_check_invalid_column(engine):
    with pytest.raises(KeyError):
        engine.run_empty_check(["nonexistent_column"])

def test_run_unique_check_invalid_column(engine):
    with pytest.raises(KeyError):
        engine.run_unique_check("nonexistent_column")

def test_run_dtype_check_invalid_dtype(engine):
    with pytest.raises(ValueError):
        engine.run_dtype_check({"age": "nonsense_dtype"})

def test_run_stringformat_check_invalid_column(engine):
    with pytest.raises(KeyError):
        engine.run_stringformat_check("nonexistent_column", r"^[A-Za-z]+$")

def test_run_range_check_invalid_column(engine):
    with pytest.raises(ValueError):
        engine.run_range_check("nonexistent_column", 0, 100)

def test_run_categoricalvalues_check_invalid_column(engine):
    with pytest.raises(ValueError):
        engine.run_categoricalvalues_check("nonexistent_column", ["A", "B"])

def test_run_statisticaldistribution_check_invalid_column(engine):
    with pytest.raises(ValueError):
        engine.run_statisticaldistribution_check("nonexistent_column", "feature_drift", {"mean": 0, "std": 1})

def test_run_statisticaldistribution_check_invalid_mode(engine):
    with pytest.raises(ValueError):
        engine.run_statisticaldistribution_check("score", "invalid_mode")

def test_run_datafreshness_check_invalid_column(engine):
    with pytest.raises(ValueError):
        engine.run_datafreshness_check("nonexistent_column", "1d")

def test_run_datafreshness_check_non_datetime(engine):
    with pytest.raises(TypeError):
        engine.run_datafreshness_check("score", "1d")

def test_run_referential_integrity_check_invalid_column(engine):
    ref_df = pd.DataFrame({"ref_id": [1, 2, 4]})
    with pytest.raises(KeyError):
        engine.run_referential_integrity_check("nonexistent_column", ref_df, "ref_id")

def test_run_referential_integrity_check_invalid_reference_column(engine):
    ref_df = pd.DataFrame({"ref_id": [1, 2, 4]})
    with pytest.raises(KeyError):
        engine.run_referential_integrity_check("id", ref_df, "nonexistent_ref_id")

def test_run_custom_check_invalid_expression(engine):
    with pytest.raises(ValueError):
        engine.run_custom_check("score", "not_a_callable")