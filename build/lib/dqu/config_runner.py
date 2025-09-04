import yaml
import json
import os
import uuid
from dqu.kernel.dataframe import DquDataFrame
from dqu.validators.dup_check import DquDupCheck
from dqu.validators.empty_check import DquEmptyCheck
from dqu.validators.unique_check import DquUniqueCheck
from dqu.validators.dtype_check import DquDtypeCheck
from dqu.validators.stringformat_check import DquStringFormatCheck
from dqu.validators.schema_check import DquSchemaValidationCheck
from dqu.validators.range_check import DquRangeCheck
from dqu.validators.categoricalvalues_check import DquCategoricalValuesCheck
from dqu.validators.statisticaldistribution_check import DquStatisticalDistributionCheck
from dqu.validators.datafreshness_check import DquDataFreshnessCheck
from dqu.validators.referential_integrity_check import DquReferentialIntegrityCheck
from dqu.validators.rowcount_check import DquRowCountCheck
from dqu.validators.custom_check import DquCustomCheck

class DquConfigRunner:
    """
    DQUConfigRunner
    ---------------
    Loads a YAML configuration file describing data quality checks,
    runs the checks on the provided DquDataFrame, and emits results
    to a uniquely named JSON file in the current directory.

    Usage:
        runner = DQUConfigRunner()
        runner.run_checks_from_yaml(dqudf, "dqu_config.yml")
    """

    @staticmethod
    def run_checks_from_yaml(dataframe, yaml_path=None, yaml_string=None, write_to_file=True, df_mapping=None):
        """
        Runs DQU checks as specified in a YAML config file or YAML string.

        Parameters:
            dataframe: DquDataFrame or pandas DataFrame
            yaml_path: Path to YAML config file (optional)
            yaml_string: YAML config as a string (optional)
            Both yaml_path and yaml_string cannot be None; one must be provided.
            write_to_file: If True, writes results to a JSON file; if False, returns results as a JSON string
            df_mapping: A dictionary mapping reference DataFrame names to actual DataFrame objects (optional, used only for referentialintegrity)

        Returns:
            If write_to_file is False, returns the results as a JSON string.
            If write_to_file is True, writes results to a file and returns nothing.

        Raises:
            Exception with a helpful message if YAML or check config is invalid.
        """
        # Ensure dataframe is DquDataFrame
        if not isinstance(dataframe, DquDataFrame):
            dataframe = DquDataFrame(dataframe)

        # Only allow pandas, spark, ray engines for YAML-based checks
        allowed_engines = {"pandas", "spark", "ray"}
        engine = dataframe.get_engine() if hasattr(dataframe, "get_engine") else None
        if engine not in allowed_engines:
            raise Exception(f"YAML-based checks are only supported for pandas, spark, and ray engines. Detected engine: {engine}")

        # Load YAML config from file or string
        try:
            if yaml_string is not None:
                config = yaml.safe_load(yaml_string)
            elif yaml_path is not None:
                with open(yaml_path, "r") as f:
                    config = yaml.safe_load(f)
            else:
                raise Exception("Either yaml_path or yaml_string must be provided.")
        except Exception as e:
            raise Exception(f"Failed to load YAML config: {e}")

        run_id = config.get("run_id")
        checks = config.get("checks", [])
        results = []

        for idx, check_cfg in enumerate(checks):
            try:
                check_type = check_cfg["type"].lower()
                if check_type == "duplicate":
                    check = DquDupCheck(dataframe, config=check_cfg, run_id=run_id)
                elif check_type == "empty":
                    check = DquEmptyCheck(dataframe, config=check_cfg, run_id=run_id)
                elif check_type == "unique":
                    check = DquUniqueCheck(dataframe, config=check_cfg, run_id=run_id)
                elif check_type == "dtype":
                    check = DquDtypeCheck(dataframe, config=check_cfg, run_id=run_id)
                elif check_type == "stringformat":
                    check = DquStringFormatCheck(dataframe, config=check_cfg, run_id=run_id)
                elif check_type == "schemavalidation":
                    check = DquSchemaValidationCheck(dataframe, config=check_cfg, run_id=run_id)
                elif check_type == "range":
                    check = DquRangeCheck(dataframe, config=check_cfg, run_id=run_id)
                elif check_type == "categoricalvalues":
                    check = DquCategoricalValuesCheck(dataframe, config=check_cfg, run_id=run_id)
                elif check_type == "statisticaldistribution":
                    check = DquStatisticalDistributionCheck(dataframe, config=check_cfg, run_id=run_id)
                elif check_type == "datafreshness":
                    check = DquDataFreshnessCheck(dataframe, config=check_cfg, run_id=run_id)
                elif check_type == "referentialintegrity":
                    ref_df_name = check_cfg.get("reference_df")
                    if isinstance(ref_df_name, str) and df_mapping and ref_df_name in df_mapping:
                        check_cfg["reference_df"] = df_mapping[ref_df_name]
                    check = DquReferentialIntegrityCheck(dataframe, config=check_cfg, run_id=run_id)
                elif check_type == "rowcount":
                    check = DquRowCountCheck(dataframe, config=check_cfg, run_id=run_id)
                elif check_type == "custom":
                    func_str = check_cfg.get("func")
                    if isinstance(func_str, str):
                        check_cfg["func"] = eval(func_str)
                    check = DquCustomCheck(dataframe, config=check_cfg, run_id=run_id)
                else:
                    raise ValueError(f"Unknown check type: {check_type}")

                result = check.run(evaluation="basic")
                result_dict = json.loads(result) if isinstance(result, str) else result
                # Before appending result_dict, sanitize dqu_eval_config
                eval_config = dict(check_cfg)  # shallow copy
                # Handle reference_df serialization
                if "reference_df" in eval_config:
                    eval_config["reference_df"] = ref_df_name
                # Handle func serialization for custom checks
                if "func" in eval_config:
                    eval_config["func"] = func_str
                result_dict["dqu_eval_config"] = eval_config
                results.append(result_dict)
            except Exception as e:
                results.append({
                    "status": "Error",
                    "check_index": idx,
                    "check_config": check_cfg,
                    "error_message": str(e),
                    "run_id": run_id
                })

        if write_to_file:
            results_path = f"dqu_check_results-{uuid.uuid4()}.json"
            with open(results_path, "w") as f:
                json.dump(results, f, indent=2)
            print(f"DQU check results written to {os.path.abspath(results_path)}")
            return
        else:
            return json.dumps(results, indent=2)
