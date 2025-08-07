import pytest
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from dqu.kernel.engine.spark_engine import SparkEngine
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("dqu-test").getOrCreate()

@pytest.fixture
def sample_spark_df(spark):
    data = [
        (1, "Alice", 25, 90, "A", datetime(2023, 1, 1)),
        (2, "Bob", 30, 85, "B", datetime(2023, 1, 2)),
        (2, "Bob", 30, 85, "B", datetime(2023, 1, 2)),
        (4, "", 0, 70, "C", datetime(2023, 1, 3)),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("score", IntegerType(), False),
        StructField("category", StringType(), False),
        StructField("date", TimestampType(), False),
    ])
    return spark.createDataFrame(data, schema=schema)

@pytest.fixture
def engine(sample_spark_df):
    return SparkEngine(sample_spark_df)

# ------------------- POSITIVE TESTS -------------------

def test_run_dup_check(engine):
    result, df = engine.run_dup_check(["id"], evaluation="advanced")
    assert "Failed" in result
    assert df.count() > 0

def test_run_empty_check(engine):
    result, df = engine.run_empty_check(["name", "age"], evaluation="advanced")
    assert "dqu_failed_count" in result
    assert df.count() == 1

def test_run_unique_check(engine):
    result, df = engine.run_unique_check("id", evaluation="advanced")
    assert "Failed" in result
    assert df.count() > 0

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
    result = engine.run_statisticaldistribution_check("score", "feature_drift", reference_stats, tolerance=20)
    assert "dqu_drift_mean" in result

def test_run_statisticaldistribution_check_label_balance(engine):
    result = engine.run_statisticaldistribution_check("category", "label_balance")
    assert "dqu_distribution" in result

def test_run_datafreshness_check(engine):
    result = engine.run_datafreshness_check("date", "2d")
    assert "latest_timestamp" in result

def test_run_referential_integrity_check(engine, spark):
    ref_df = spark.createDataFrame(pd.DataFrame({"ref_id": [1, 2, 4]}))
    result, df = engine.run_referential_integrity_check("id", ref_df, "ref_id", evaluation="advanced")
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

def test_run_referential_integrity_check_invalid_column(engine, spark):
    ref_df = spark.createDataFrame(pd.DataFrame({"ref_id": [1, 2, 4]}))
    with pytest.raises(Exception):
        engine.run_referential_integrity_check("nonexistent_column", ref_df, "ref_id")

def test_run_referential_integrity_check_invalid_reference_column(engine, spark):
    ref_df = spark.createDataFrame(pd.DataFrame({"ref_id": [1, 2, 4]}))
    with pytest.raises(Exception):
        engine.run_referential_integrity_check("id", ref_df, "nonexistent_ref_id")

def test_run_custom_check_invalid_expression(engine):
    with pytest.raises(Exception):
        engine.run_custom_check("score", "not_a_callable")