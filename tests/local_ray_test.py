"""
import ray
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.dup_check import DquDupCheck
from datetime import datetime

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

data = [
    {"id": 1, "name": "Alice", "age": 25, "score": 90, "category": "A", "date": datetime(2023, 1, 1)},
    {"id": 2, "name": "Bob", "age": 30, "score": 85, "category": "B", "date": datetime(2023, 1, 2)},
    {"id": 2, "name": "Bob", "age": 30, "score": 85, "category": "B", "date": datetime(2023, 1, 2)},  # duplicate
    {"id": 4, "name": "", "age": 0, "score": 70, "category": "C", "date": datetime(2023, 1, 3)},
]
ds = ray.data.from_items(data)
dqudf = DquDataFrame(ds)
check = DquDupCheck(
    dqudf,
    config={"columns": ["id"]},
    run_id="fe6e3991-6e3f-4f53-83cc-b8399bc8fc28"
)

result, failed_rows = check.run(evaluation="advanced")
print(result)
failed_rows.show(limit=10)

import ray
from datetime import datetime
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.empty_check import DquEmptyCheck

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

data = [
    {"id": 1, "category": "A"},
    {"id": None, "category": "B"},
    {"id": 3, "category": "C"},
]

ds = ray.data.from_items(data)
dqudf = DquDataFrame(ds)

check = DquEmptyCheck(
    dqudf,
    config={"columns": ["id"]},
    run_id="fe6e3991-6e3f-4f53-83cc-b8399bc8fc28"
)

result, failed_rows = check.run(evaluation="advanced")
print(result)
failed_rows.show(limit=10)
"""
import ray
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.unique_check import DquUniqueCheck

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

data = [
    {"id": 1, "name": "Daniel"},
    {"id": 2, "name": "Bob"},
    {"id": 2, "name": "Bob"},  # Duplicate ID
    {"id": 3, "name": "Dave"},
]
ds = ray.data.from_items(data)
dqudf = DquDataFrame(ds)
check = DquUniqueCheck(
    dqudf,
    config={"columns": ["id"]},
    run_id="fe6e3991-6e3f-4f53-83cc-b8399bc8fc28"
)

result, failed_rows = check.run(evaluation="advanced")
print(result)
failed_rows.show(limit=10)
"""
import ray
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.dtype_check import DquDtypeCheck

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)


data = [
    {"id": 1, "name": "Daniel"},
    {"id": 2, "name": "Bob"},
    {"id": 2, "name": "Bob"},
    {"id": 3, "name": "Dave"},
]

ds = ray.data.from_items(data)
dqudf = DquDataFrame(ds)

expected_types = {
    "id": "int",
    "name": "str"
}

check = DquDtypeCheck(
    dqudf,
    config={"columns": expected_types},
    run_id="fe6e3991-6e3f-4f53-83cc-b8399bc8fc28"
)

result, failed_rows = check.run(evaluation="advanced")
print(result)
failed_rows.show(limit=10)

import ray
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.stringformat_check import DquStringFormatCheck

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

data = [
    {"id": 1, "name": "Daniel"},
    {"id": 2, "name": "Bob123"},
    {"id": 3, "name": "Alice!"},
    {"id": 4, "name": "Dave"},
]

ds = ray.data.from_items(data)
dqudf = DquDataFrame(ds)
check = DquStringFormatCheck(
    dqudf,
    config={
        "column": "name",
        "pattern": r"^[A-Za-z]+$"  # regex for alphabets only
    },
    run_id="fe6e3991-6e3f-4f53-83cc-b8399bc8fc28"
)

result, failed_rows = check.run(evaluation="advanced")
print(result)
failed_rows.show(limit=10)

import ray
from datetime import datetime
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.schema_check import DquSchemaValidationCheck

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

data = [
    {"id": 1, "name": "Alice", "age": "25", "signup_date": datetime(2023, 1, 1)},
    {"id": 2, "name": "Bob", "age": "30", "signup_date": datetime(2023, 2, 15)},
    {"id": 3, "name": "Charlie", "age": "35", "signup_date": datetime(2023, 3, 20)}
]

ds = ray.data.from_items(data)
dqudf = DquDataFrame(ds)
expected_schema = {
    "id": "int64",
    "name": "string",
    "age": "int64",           # Mismatch here
    "signup_date": "timestamp[s]"
}

check = DquSchemaValidationCheck(
    dqudf,
    config={"expected_schema": expected_schema},
    run_id="a45b0020-f5b0-4a59-b418-0d9f7c3700a6"
)

result = check.run(evaluation="advanced")
print(result)

import ray
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.range_check import DquRangeCheck


if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

data = [
    {"id": 1, "name": "Alice", "age": 22},
    {"id": 2, "name": "Bob", "age": 17},        # Out of range
    {"id": 3, "name": "Charlie", "age": 65},
    {"id": 4, "name": "Daisy", "age": 80}       # Out of range
]

ds = ray.data.from_items(data)
dqudf = DquDataFrame(ds)

check = DquRangeCheck(
    dqudf,
    config={"column": "age", "min": 18, "max": 65},
    run_id="cb376a28-2652-4cb3-a41f-5ec9c0b93dc6"
)

result, failed_rows = check.run(evaluation="advanced")
print(result)
failed_rows.show(limit=10)

import ray
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.categoricalvalues_check import DquCategoricalValuesCheck

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

data = [
    {"employee_id": 101, "name": "Alice", "department": "HR"},
    {"employee_id": 102, "name": "Bob", "department": "Finance"},
    {"employee_id": 103, "name": "Charlie", "department": "Legal"},
    {"employee_id": 104, "name": "Daisy", "department": "Magic"}  # Invalid
]

ds = ray.data.from_items(data)
dqudf = DquDataFrame(ds)
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
failed_rows.show(limit=10)

import ray
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.statisticaldistribution_check import DquStatisticalDistributionCheck

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

data = [
    {"employee_id": 1, "salary": 70000.0},
    {"employee_id": 2, "salary": 72000.0},
    {"employee_id": 3, "salary": 71000.0},
    {"employee_id": 4, "salary": 69000.0},
    {"employee_id": 5, "salary": 130000.0}  # drifted
]

ds = ray.data.from_items(data)
dqudf = DquDataFrame(ds)
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
        "tolerance": 0.1  # Allow 10% deviation
    },
    run_id="2a9b9370-2b7f-4d01-bd87-57b4c11c86be"
)

result = check.run(evaluation="advanced")
print(result)

import ray
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.statisticaldistribution_check import DquStatisticalDistributionCheck

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

data = [
    {"id": 101, "label": "spam"},
    {"id": 102, "label": "spam"},
    {"id": 103, "label": "ham"},
    {"id": 104, "label": "spam"},
    {"id": 105, "label": "spam"},
    {"id": 106, "label": "spam"},
    {"id": 107, "label": "spam"},
    {"id": 108, "label": "ham"},
]

ds = ray.data.from_items(data)
dqudf = DquDataFrame(ds)

check = DquStatisticalDistributionCheck(
    dqudf,
    config={
        "column": "label",
        "mode": "label_balance"
    },
    run_id="c8395aa5-ecf0-40c0-9cc9-0a99e22f73a9"
)

result = check.run(evaluation="advanced")
print(result)

import ray
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.statisticaldistribution_check import DquStatisticalDistributionCheck

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

# imbalanced data (95% spam, 5% ham)
data = [{"id": i, "label": "spam"} for i in range(1, 20)] + [{"id": 20, "label": "ham"}]

ds = ray.data.from_items(data)
dqudf = DquDataFrame(ds)

check = DquStatisticalDistributionCheck(
    dqudf,
    config={
        "column": "label",
        "mode": "label_balance"
    },
    run_id="c8395aa5-ecf0-40c0-9cc9-0a99e22f73a9"
)

result = check.run(evaluation="advanced")
print(result)

import ray
from datetime import datetime
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.datafreshness_check import DquDataFreshnessCheck

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

now = datetime.utcnow()
data = [
    {"id": 1, "created_at": now},
    {"id": 2, "created_at": now},
    {"id": 3, "created_at": now},
    {"id": 4, "created_at": now}
]

ds = ray.data.from_items(data)
dqudf = DquDataFrame(ds)

# Run data freshness check with 2-day threshold
check = DquDataFreshnessCheck(
    dqudf,
    config={
        "column": "created_at",
        "freshness_threshold": "2d"  # 'h' for Hours
    },
    run_id="c8395aa5-ecf0-40c0-9cc9-0a99e22f73a9"
)

result = check.run(evaluation="advanced")
print(result)

import ray
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.referential_integrity_check import DquReferentialIntegrityCheck

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

# Main Ray Dataset (e.g., transactions)
main_data = [
    {"user_id": 101, "amount": 50.0},
    {"user_id": 102, "amount": 75.5},
    {"user_id": 103, "amount": 23.0},
    {"user_id": 104, "amount": 99.9}
]
main_ds = ray.data.from_items(main_data)
dqu_main = DquDataFrame(main_ds)

# Reference Ray Dataset (e.g., users)
ref_data = [
    {"id": 101},
    {"id": 102},
    {"id": 103},
    {"id": 107}
]
ref_ds = ray.data.from_items(ref_data)
dqu_ref = DquDataFrame(ref_ds)

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
failed_rows.show(limit=10)

import ray
from ray.data import from_items
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.rowcount_check import DquRowCountCheck

if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)
data = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
    {"id": 3, "name": "Charlie"},
    {"id": 4, "name": "David"},
    {"id": 5, "name": "Eva"}
]

ds = from_items(data)
dqudf = DquDataFrame(ds)

# Row count check between 3 and 10
check = DquRowCountCheck(
    dqudf,
    config={
        "min": 30,
        "max": 100
    },
    run_id="123e4567-e89b-12d3-a456-426614174000"
)

result = check.run(evaluation="advanced")
print(result)

import ray
from ray.data import from_items
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.custom_check import DquCustomCheck
if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

data = [
    {"id": 1, "score": 85},
    {"id": 2, "score": 92},
    {"id": 3, "score": -10},  # This should fail
    {"id": 4, "score": 74}
]

ds = from_items(data)
dqudf = DquDataFrame(ds)

# Custom check: score must be >= 0
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
failed_rows.show()


import ray
from ray.data import from_items
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.custom_check import DquCustomCheck
if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

data = [
    {"id": 1, "age": 25, "country": "US"},
    {"id": 2, "age": 17, "country": "US"},  # Fails: age < 18
    {"id": 3, "age": 35, "country": "CA"},  # Fails: country != US
    {"id": 4, "age": 45, "country": "IN"}   # Fails: country != US
]

ds = from_items(data)
dqudf = DquDataFrame(ds)

# Apply custom row-level check
check = DquCustomCheck(
    dqudf,
    config={
        "func": lambda row: row["age"] >= 18 and row["country"] == "US"
    },
    run_id="123e4567-e89b-12d3-a456-426614174000"
)
result, failed_rows = check.run(evaluation="advanced")
print(result)
failed_rows.show()
"""

