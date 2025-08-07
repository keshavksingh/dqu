import pytest
import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.engine.flink_engine import FlinkEngine

@pytest.fixture(scope="session")
def flink_env():
    return StreamExecutionEnvironment.get_execution_environment()


def datetime_to_milliseconds(dt_obj):
    timestamp_seconds = dt_obj.timestamp()
    return int(timestamp_seconds * 1000)

@pytest.fixture
def sample_flink_df(flink_env):
    now_ms = datetime_to_milliseconds(datetime.datetime.utcnow())
    data = [
        (1, "Alice", 25, 90, "A",now_ms),
        (2, "Bob", 30, 85, "B",now_ms),
        (2, "Bob", 30, 85, "B",now_ms),
        (4, "", 0, 70, "C",now_ms),
    ]
    columns = ["id", "name", "age", "score", "category","modifiedtime"]
    type_info = Types.ROW([
        Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.STRING(), Types.LONG()
    ])
    ds = flink_env.from_collection(collection=data, type_info=type_info)
    return ds, columns

@pytest.fixture
def engine(sample_flink_df):
    ds, columns = sample_flink_df
    return FlinkEngine(ds, columns=columns)

# ------------------- POSITIVE TESTS -------------------

def test_run_dup_check(engine):
    result, df = engine.run_dup_check(["id"], evaluation="advanced")
    assert "Failed" in result
    assert df is not None


def test_run_empty_check(engine):
    result, df = engine.run_empty_check(["name", "age"], evaluation="advanced")
    assert "dqu_failed_count" in result
    assert df is not None

def test_run_unique_check(engine):
    result, df = engine.run_unique_check("id", evaluation="advanced")
    assert "Failed" in result
    assert df is not None

def test_run_dtype_check(engine):
    result, df = engine.run_dtype_check({"age": "int"}, evaluation="advanced")
    assert "dqu_failed_count" in result

def test_run_stringformat_check(engine):
    result, df = engine.run_stringformat_check("name", r"^[A-Za-z]+$", evaluation="advanced")
    assert "dqu_failed_count" in result

def test_run_schemavalidation_check(engine):
    expected_schema = {"id": "int", "name": "string", "age": "int", "score": "int", "category": "string", "date": "timestamp"}
    result = engine.run_schemavalidation_check(expected_schema)
    assert "status" in result

def test_run_range_check(engine):
    result, df = engine.run_range_check("score", 80, 100, evaluation="advanced")
    assert "dqu_failed_count" in result

def test_run_categoricalvalues_check(engine):
    result, df = engine.run_categoricalvalues_check("category", ["A", "B"], evaluation="advanced")
    assert "dqu_failed_count" in result

def test_run_statisticaldistribution_check_feature_drift(engine):
    reference_stats = {"mean": 80, "std": 10}
    with pytest.raises(NotImplementedError, match="Feature drift not supported"):
        engine.run_statisticaldistribution_check("score", "feature_drift", reference_stats, tolerance=20)

def test_run_statisticaldistribution_check_label_balance(engine):
    result = engine.run_statisticaldistribution_check("category", "label_balance")
    assert "dqu_distribution" in result

def test_run_datafreshness_check(engine):
    result = engine.run_datafreshness_check("modifiedtime", "2d")
    assert "latest_timestamp" in result

def test_run_referential_integrity_check(engine, flink_env):
    ref_data = [(1,), (2,), (4,)]
    ref_type_info = Types.ROW([Types.INT()])
    ref_df = flink_env.from_collection(collection=ref_data, type_info=ref_type_info)
    result, df = engine.run_referential_integrity_check("id", ref_df, "f0", evaluation="advanced")
    assert "dqu_failed_count" in result

def test_run_rowcount_check(engine):
    result = engine.run_rowcount_check(min_rows=2, max_rows=10)
    assert "status" in result

def test_run_custom_check_column(engine):
    result, df = engine.run_custom_check("score", lambda x: x >= 80, evaluation="advanced")
    assert "dqu_failed_count" in result

def test_run_custom_check_row(engine):
    result, df = engine.run_custom_check(None, lambda row: row["score"] >= 70, evaluation="advanced")
    assert "dqu_failed_count" in result

# ------------------- NEGATIVE TESTS -------------------

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
    with pytest.raises(KeyError):
        engine.run_range_check("nonexistent_column", 0, 100)


def test_run_categoricalvalues_check_invalid_column(engine):
    with pytest.raises(KeyError):
        engine.run_categoricalvalues_check("nonexistent_column", ["A", "B"])


def test_run_statisticaldistribution_check_invalid_column(engine):
    with pytest.raises(KeyError):
        engine.run_statisticaldistribution_check("nonexistent_column", "feature_drift", {"mean": 0, "std": 1})


def test_run_statisticaldistribution_check_invalid_mode(engine):
    with pytest.raises(ValueError):
        engine.run_statisticaldistribution_check("score", "invalid_mode")


def test_run_datafreshness_check_invalid_column(engine):
    with pytest.raises(KeyError):
        engine.run_datafreshness_check("nonexistent_column", "1d")


def test_run_datafreshness_check_non_datetime(engine):
    # 'score' column is int, not a timestamp (milliseconds)
    with pytest.raises(TypeError):
        engine.run_datafreshness_check("score", "1d")


def test_run_referential_integrity_check_invalid_column(engine, flink_env):
    ref_data = [(1,), (2,), (4,)]
    ref_type_info = Types.ROW([Types.INT()])
    ref_df = flink_env.from_collection(collection=ref_data, type_info=ref_type_info)
    with pytest.raises(KeyError):
        engine.run_referential_integrity_check("nonexistent_column", ref_df, "f0")


def test_run_referential_integrity_check_invalid_reference_column(engine, flink_env):
    ref_data = [(1,), (2,), (4,)]
    ref_type_info = Types.ROW([Types.INT()])
    ref_df = flink_env.from_collection(collection=ref_data, type_info=ref_type_info)
    with pytest.raises(KeyError):
        engine.run_referential_integrity_check(
            column="id",
            reference_df=ref_df,
            reference_column="nonexistent_column", 
            reference_columns=["f0"],
            evaluation="basic"
        )


def test_run_custom_check_invalid_expression(engine):
    with pytest.raises(TypeError):
        engine.run_custom_check("score", "not_a_callable")
