"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.dup_check import DquDupCheck
from datetime import datetime

def datetime_to_milliseconds(dt_obj):
    return int(dt_obj.timestamp() * 1000)

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

data = [
    (1, "Alice", 25, 90, "A", datetime_to_milliseconds(datetime(2023, 1, 1))),
    (2, "Bob", 30, 85, "B", datetime_to_milliseconds(datetime(2023, 1, 2))),
    (2, "Bob", 30, 85, "B", datetime_to_milliseconds(datetime(2023, 1, 2))),  # duplicate
    (4, "", 0, 70, "C", datetime_to_milliseconds(datetime(2023, 1, 3))),
]

columns = ["id", "name", "age", "score", "category", "date"]
type_info = Types.ROW([
    Types.INT(),     # id
    Types.STRING(),  # name
    Types.INT(),     # age
    Types.INT(),     # score
    Types.STRING(),  # category
    Types.LONG()     # date (timestamp in ms)
])

ds = env.from_collection(data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)
check = DquDupCheck(
    dqudf,
    config={"columns": ["id"]},
    run_id="fe6e3991-6e3f-4f53-83cc-b8399bc8fc28"
)

result, failed_rows = check.run(evaluation="advanced")

print(result)
print(failed_rows)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.empty_check import DquEmptyCheck

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
data = [
    (1, "A"),
    (None, "B"),  # This should fail the empty check
    (3, "C")
]
columns = ["id", "category"]
type_info = Types.ROW([
    Types.INT(),      # id
    Types.STRING()    # category
])
ds = env.from_collection(data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)
check = DquEmptyCheck(
    dqudf,
    config={"columns": ["id"]},
    run_id="fe6e3991-6e3f-4f53-83cc-b8399bc8fc28"
)

result, failed_rows = check.run(evaluation="advanced")
print(result)
print(failed_rows)
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.unique_check import DquUniqueCheck

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
data = [
    (1, "Daniel"),
    (2, "Bob"),
    (2, "Bob"),  # Duplicate ID
    (3, "Dave")
]

columns = ["id", "name"]
type_info = Types.ROW([
    Types.INT(),      # id
    Types.STRING()    # name
])

ds = env.from_collection(data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)
check = DquUniqueCheck(
    dqudf,
    config={"columns": ["id"]},
    run_id="fe6e3991-6e3f-4f53-83cc-b8399bc8fc28"
)

result, failed_rows = check.run(evaluation="advanced")
print(result)
print(failed_rows)
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.range_check import DquRangeCheck

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

data = [(25,), (50,), (200,)]
columns = ["temperature"]
type_info = Types.ROW([Types.INT()])

ds = env.from_collection(data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)

check = DquRangeCheck(
    dqudf,
    config={"column": "temperature", "min": 10, "max": 100},
    run_id="fe6e3991-6e3f-4f53-83cc-b8399bc8fc28"
)

result, failed = check.run(evaluation="advanced")
print(result)
print(failed)  # or use collect() if applicable

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.dtype_check import DquDtypeCheck

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

data = [
    (1, "Daniel"),
    (2, "Bob"),
    (2, "Bob"),
    (3, "Dave")
]

columns = ["id", "name"]
type_info = Types.ROW([
    Types.INT(),      # id
    Types.STRING()    # name
])
ds = env.from_collection(data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)
expected_types = {
    "id": "int",
    "name": "int"
}

check = DquDtypeCheck(
    dqudf,
    config={"columns": expected_types},
    run_id="fe6e3991-6e3f-4f53-83cc-b8399bc8fc28"
)

result, failed_rows = check.run(evaluation="advanced")
print(result)
print(failed_rows)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.stringformat_check import DquStringFormatCheck

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

data = [
    (1, "Daniel"),     # ✅ valid
    (2, "Bob123"),     # ❌ has digits
    (3, "Alice!"),     # ❌ has special char
    (4, "Dave")        # ✅ valid
]

columns = ["id", "name"]
type_info = Types.ROW([
    Types.INT(),      # id
    Types.STRING()    # name
])
ds = env.from_collection(data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)
check = DquStringFormatCheck(
    dqudf,
    config={
        "column": "name",
        "pattern": r"^[A-Za-z]+$"  # only letters allowed
    },
    run_id="fe6e3991-6e3f-4f53-83cc-b8399bc8fc28"
)

result, failed_rows = check.run(evaluation="advanced")
print(result)
print(failed_rows)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.schema_check import DquSchemaValidationCheck
import datetime

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
data = [
    (1, "Alice", "25", int(datetime.datetime(2023, 1, 1).timestamp())),
    (2, "Bob", "30", int(datetime.datetime(2023, 2, 15).timestamp())),
    (3, "Charlie", "35", int(datetime.datetime(2023, 3, 20).timestamp()))
]

columns = ["id", "name", "age", "signup_date"]
type_info = Types.ROW([
    Types.INT(),        # id
    Types.STRING(),     # name
    Types.STRING(),     # age (should be INT based on expected schema – mismatch)
    Types.LONG()        # signup_date as UNIX timestamp (to simulate timestamp[s])
])
ds = env.from_collection(data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)
expected_schema = {
    "id": "int",
    "name": "str",
    "age": "int",              # ← Mismatch on purpose: actual is string
    "signup_date": "timestamp[s]"
}
check = DquSchemaValidationCheck(
    dqudf,
    config={"expected_schema": expected_schema},
    run_id="a45b0020-f5b0-4a59-b418-0d9f7c3700a6"
)

result = check.run(evaluation="advanced")
print(result)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.categoricalvalues_check import DquCategoricalValuesCheck

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
data = [
    (101, "Alice", "HR"),
    (102, "Bob", "Finance"),
    (103, "Charlie", "Legal"),
    (104, "Daisy", "Magic")  # Invalid value
]

columns = ["employee_id", "name", "department"]
type_info = Types.ROW([
    Types.INT(),        # employee_id
    Types.STRING(),     # name
    Types.STRING()      # department
])

ds = env.from_collection(data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)
check = DquCategoricalValuesCheck(
    dqudf,
    config={
        "column": "department",
        "allowed_values": ["HR", "Finance", "Legal", "Engineering"]
    },
    run_id="ec5f4c82-d7a4-43de-9ff5-c18186a684f2"
)

result, failed_rows = check.run(evaluation="advanced")

print(result)
print(failed_rows)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.statisticaldistribution_check import DquStatisticalDistributionCheck

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

data = [
    (1, 70000.0),
    (2, 72000.0),
    (3, 71000.0),
    (4, 69000.0),
    (5, 130000.0)  # drifted
]

columns = ["employee_id", "salary"]
type_info = Types.ROW([
    Types.INT(),      # employee_id
    Types.FLOAT()     # salary
])

ds = env.from_collection(data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)
reference_stats = {
    "mean": 70000,
    "std": 5000
}

check = DquStatisticalDistributionCheck(
    dqudf,
    config={
        "column": "salary",
        "mode": "feature_drift",
        "reference_stats": reference_stats,
        "tolerance": 0.1  # 10% deviation allowed
    },
    run_id="2a9b9370-2b7f-4d01-bd87-57b4c11c86be"
)

result = check.run(evaluation="advanced")
print(result)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from datetime import datetime, timedelta
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.datafreshness_check import DquDataFreshnessCheck

def datetime_to_milliseconds(dt_obj):
    return int(dt_obj.timestamp() * 1000)

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

now = datetime.utcnow()
past_time = now + timedelta(days=-3)
timestamp_ms = datetime_to_milliseconds(past_time)

data = [
    (1, timestamp_ms),
    (2, timestamp_ms),
    (3, timestamp_ms),
    (4, timestamp_ms)
]

columns = ["id", "created_at"]
type_info = Types.ROW([
    Types.INT(),     # id
    Types.LONG()     # created_at (epoch milliseconds)
])

ds = env.from_collection(data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)
check = DquDataFreshnessCheck(
    dqudf,
    config={
        "column": "created_at",
        "freshness_threshold": "2d"  # Accepts 'd' (days), 'h' (hours), 'm' (minutes)
    },
    run_id="c8395aa5-ecf0-40c0-9cc9-0a99e22f73a9"
)

result = check.run(evaluation="advanced")
print(result)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.referential_integrity_check import DquReferentialIntegrityCheck

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Main dataset (e.g., transactions)
main_data = [
    (101, 50.0),
    (102, 75.5),
    (103, 23.0),
    (104, 99.9),
]
main_columns = ["user_id", "amount"]
main_type_info = Types.ROW([Types.INT(), Types.FLOAT()])
main_ds = env.from_collection(main_data, type_info=main_type_info)
dqu_main = DquDataFrame(main_ds, columns=main_columns)

# 3. Reference dataset (e.g., users)
ref_data = [
    (101,),
    (102,),
    (103,),
    (104,),
]
ref_columns = ["id"]
ref_type_info = Types.ROW([Types.INT()])
ref_ds = env.from_collection(ref_data, type_info=ref_type_info)
dqu_ref = DquDataFrame(ref_ds, columns=ref_columns)

check = DquReferentialIntegrityCheck(
    dqu_main,
    config={
        "column": "user_id",
        "reference_df": dqu_ref.df,
        "reference_column": "id"
    },
    run_id="c8395aa5-ecf0-40c0-9cc9-0a99e22f73a9"
)
result, failed_rows = check.run(evaluation="advanced")
print(result)
print(failed_rows)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.rowcount_check import DquRowCountCheck

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie"),
    (4, "David"),
    (5, "Eva")
]

columns = ["id", "name"]
type_info = Types.ROW([
    Types.INT(),         # id
    Types.STRING()       # name
])

ds = env.from_collection(collection=data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)
check = DquRowCountCheck(
    dqudf,
    config={
        "min": 2,
        "max": 4
    },
    run_id="123e4567-e89b-12d3-a456-426614174000"
)
result = check.run(evaluation="advanced")
print(result)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.custom_check import DquCustomCheck

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
data = [
    (1, 85),
    (2, 92),
    (3, -10),  # Should fail
    (4, 74)
]

columns = ["id", "score"]
type_info = Types.ROW([
    Types.INT(),  # id
    Types.INT()   # score
])

ds = env.from_collection(data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)
check = DquCustomCheck(
    dqudf,
    config={
        "column": "score",
        "func": lambda x: x >= 0
    },
    run_id="123e4567-e89b-12d3-a456-426614174000"
)

result, failed_rows = check.run(evaluation="advanced")
print(result)
print(failed_rows)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.custom_check import DquCustomCheck

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
data = [
    (1, 25, "US"),
    (2, 17, "US"),   # Fails: age < 18
    (3, 35, "CA"),   # Fails: country != US
    (4, 45, "IN")    # Fails: country != US
]

columns = ["id", "age", "country"]
type_info = Types.ROW([
    Types.INT(),  # id
    Types.INT(),   # score
    Types.STRING()  # country
])

ds = env.from_collection(data, type_info=type_info)
dqudf = DquDataFrame(ds, columns=columns)
check = DquCustomCheck(
    dqudf,
    config={
        "func": lambda row: row["age"] >= 18 and row["country"] == "US"
    },
    run_id="123e4567-e89b-12d3-a456-426614174000"
)
result, failed_rows = check.run(evaluation="advanced")
print(result)
print(failed_rows)
"""
