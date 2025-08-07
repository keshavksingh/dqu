import pytest
from datetime import datetime
import ray
from ray.data import Dataset as RayDataset
from dqu.kernel.engine.ray_engine import RayEngine

@pytest.fixture(scope="session", autouse=True)
def ray_init():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    yield
    ray.shutdown()

@pytest.fixture
def sample_ray_df(ray_init):
    data = [
        {"id": 1, "name": "Alice", "age": 25, "score": 90, "category": "A", "date": datetime(2023, 1, 1)},
        {"id": 2, "name": "Bob", "age": 30, "score": 85, "category": "B", "date": datetime(2023, 1, 2)},
        {"id": 2, "name": "Bob", "age": 30, "score": 85, "category": "B", "date": datetime(2023, 1, 2)},
        {"id": 4, "name": "", "age": 0, "score": 70, "category": "C", "date": datetime(2023, 1, 3)},
    ]
    ds = ray.data.from_items(data)
    return ds

@pytest.fixture
def engine(sample_ray_df):
    return RayEngine(sample_ray_df)

# ------------------- POSITIVE TESTS -------------------

def test_run_dup_check(engine):
    result, ds = engine.run_dup_check(["id"], evaluation="advanced")
    assert "Failed" in result
    assert ds.count() > 0

def test_run_empty_check(engine):
    result, ds = engine.run_empty_check(["name", "age"], evaluation="advanced")
    assert "dqu_failed_count" in result
    assert ds.count() == 1

def test_run_unique_check(engine):
    result, ds = engine.run_unique_check("id", evaluation="advanced")
    assert "Failed" in result
    assert ds.count() > 0

def test_run_dtype_check(engine):
    result, ds = engine.run_dtype_check({"age": "int"}, evaluation="advanced")
    assert "dqu_failed_count" in result

def test_run_stringformat_check(engine):
    result, ds = engine.run_stringformat_check("name", r"^[A-Za-z]+$", evaluation="advanced")
    assert "dqu_failed_count" in result

def test_run_schemavalidation_check(engine):
    expected_schema = {"id": "int", "name": "string", "age": "int", "score": "int", "category": "string", "date": "datetime"}
    result = engine.run_schemavalidation_check(expected_schema)
    assert "status" in result

def test_run_range_check(engine):
    result, ds = engine.run_range_check("score", 80, 100, evaluation="advanced")
    assert "dqu_failed_count" in result

def test_run_categoricalvalues_check(engine):
    result, ds = engine.run_categoricalvalues_check("category", ["A", "B"], evaluation="advanced")
    assert "dqu_failed_count" in result

def test_run_statisticaldistribution_check_feature_drift(engine):
    reference_stats = {"mean": 80, "std": 10}
    result = engine.run_statisticaldistribution_check("score", "feature_drift", reference_stats, tolerance=20)
    assert "dqu_drift_mean" in result

def test_run_statisticaldistribution_check_label_balance(engine):
    result = engine.run_statisticaldistribution_check("category", "label_balance")
    assert "dqu_distribution" in result

def test_run_datafreshness_check(engine):
    result = engine.run_datafreshness_check("date", "2d")
    assert "latest_timestamp" in result

def test_run_referential_integrity_check(engine):
    ref_data = [{"ref_id": 1}, {"ref_id": 2}, {"ref_id": 4}]
    ref_ds = ray.data.from_items(ref_data)
    result, ds = engine.run_referential_integrity_check("id", ref_ds, "ref_id", evaluation="advanced")
    assert "dqu_failed_count" in result

def test_run_rowcount_check(engine):
    result = engine.run_rowcount_check(min_rows=2, max_rows=10)
    assert "status" in result

def test_run_custom_check_column(engine):
    result, ds = engine.run_custom_check("score", lambda x: x >= 80, evaluation="advanced")
    assert "dqu_failed_count" in result

def test_run_custom_check_row(engine):
    result, ds = engine.run_custom_check(None, lambda row: row["score"] >= 70, evaluation="advanced")
    assert "dqu_failed_count" in result

# ------------------- NEGATIVE TESTS -------------------

def test_run_dup_check_invalid_column(engine):
    with pytest.raises(Exception):
        engine.run_dup_check(["nonexistent_column"])

def test_run_empty_check_invalid_column(engine):
    with pytest.raises(Exception):
        engine.run_empty_check(["nonexistent_column"])

def test_run_unique_check_invalid_column(engine):
    with pytest.raises(Exception):
        engine.run_unique_check("nonexistent_column")

def test_run_dtype_check_invalid_dtype(engine):
    with pytest.raises(ValueError):
        engine.run_dtype_check({"age": "nonsense_dtype"})

def test_run_stringformat_check_invalid_column(engine):
    with pytest.raises(Exception):
        engine.run_stringformat_check("nonexistent_column", r"^[A-Za-z]+$")

def test_run_range_check_invalid_column(engine):
    with pytest.raises(Exception):
        engine.run_range_check("nonexistent_column", 0, 100)

def test_run_categoricalvalues_check_invalid_column(engine):
    with pytest.raises(Exception):
        engine.run_categoricalvalues_check("nonexistent_column", ["A", "B"])

def test_run_statisticaldistribution_check_invalid_column(engine):
    with pytest.raises(Exception):
        engine.run_statisticaldistribution_check("nonexistent_column", "feature_drift", {"mean": 0, "std": 1})

def test_run_statisticaldistribution_check_invalid_mode(engine):
    with pytest.raises(ValueError):
        engine.run_statisticaldistribution_check("score", "invalid_mode")

def test_run_datafreshness_check_invalid_column(engine):
    with pytest.raises(Exception):
        engine.run_datafreshness_check("nonexistent_column", "1d")

def test_run_datafreshness_check_non_datetime(engine):
    with pytest.raises(TypeError):
        engine.run_datafreshness_check("score", "1d")

def test_run_referential_integrity_check_invalid_column(engine):
    ref_data = [{"ref_id": 1}, {"ref_id": 2}, {"ref_id": 4}]
    ref_ds = ray.data.from_items(ref_data)
    with pytest.raises(Exception):
        engine.run_referential_integrity_check("nonexistent_column", ref_ds, "ref_id")

def test_run_referential_integrity_check_invalid_reference_column(engine):
    ref_data = [{"ref_id": 1}, {"ref_id": 2}, {"ref_id": 4}]
    ref_ds = ray.data.from_items(ref_data)
    with pytest.raises(Exception):
        engine.run_referential_integrity_check("id", ref_ds, "nonexistent_ref_id")

def test_run_custom_check_invalid_expression(engine):
    with pytest.raises(Exception):
        engine.run_custom_check("score", "not_a_callable")