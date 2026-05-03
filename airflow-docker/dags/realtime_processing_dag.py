import glob
import logging
import os
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
from airflow.sdk import get_current_context
from airflow.providers.standard.sensors.python import PythonSensor

# Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Make project imports work inside Airflow
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.batch.azure_writer import AzureWriter
from pipelines.realtime.realtime_data_processor import RealtimeDataProcessor
from pipelines.realtime.realtime_data_reader import RealtimeDataReader
from pipelines.realtime.realtime_validation_rules import RealtimeValidationRules

# Configuration
INPUT_FOLDER = "/opt/airflow/data_input_realtime/"
FAILED_FOLDER = os.path.join(INPUT_FOLDER, "failed") + "/"
SUCCEEDED_FOLDER = os.path.join(INPUT_FOLDER, "succeeded") + "/"
OUTPUT_FOLDER = "/opt/airflow/output/"
OUTPUT_REALTIME_FOLDER = os.path.join(OUTPUT_FOLDER, "realtime")
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER = "processed-data"


# List only CSV files in the root input folder (skip subfolders).
def _list_root_csv_files(folder: str) -> List[str]:
    candidate_files = glob.glob(os.path.join(folder, "*.csv"))
    root_folder = os.path.abspath(folder.rstrip("/"))
    root_files = [f for f in candidate_files if os.path.dirname(os.path.abspath(f)) == root_folder]
    return sorted(root_files)


def _move_to_failed(file_path: str) -> None:
    os.makedirs(FAILED_FOLDER, exist_ok=True)
    if not os.path.exists(file_path):
        logger.warning("Failed file already moved or missing: %s", file_path)
        return

    dest_failed = os.path.join(FAILED_FOLDER, Path(file_path).name)
    if os.path.exists(dest_failed):
        stem = Path(file_path).stem
        suffix = Path(file_path).suffix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_failed = os.path.join(FAILED_FOLDER, f"{stem}_{timestamp}{suffix}")

    shutil.move(file_path, dest_failed)
    logger.info("Moved failed file %s -> %s", Path(file_path).name, dest_failed)


def _move_to_succeeded(file_path: str) -> None:
    os.makedirs(SUCCEEDED_FOLDER, exist_ok=True)
    if not os.path.exists(file_path):
        logger.warning("Succeeded file already moved or missing: %s", file_path)
        return

    dest = os.path.join(SUCCEEDED_FOLDER, Path(file_path).name)
    if os.path.exists(dest):
        stem = Path(file_path).stem
        suffix = Path(file_path).suffix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = os.path.join(SUCCEEDED_FOLDER, f"{stem}_{timestamp}{suffix}")

    shutil.move(file_path, dest)
    logger.info("Moved succeeded file %s -> %s", Path(file_path).name, dest)


def _sensor_has_files(**context) -> bool:
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    input_file = conf.get("input_file")

    if input_file:
        file_path = os.path.join(INPUT_FOLDER, input_file)
        exists = os.path.exists(file_path) and os.path.dirname(os.path.abspath(file_path)) == os.path.abspath(INPUT_FOLDER.rstrip("/"))
        if not exists:
            logger.warning("Triggered file not found in input root: %s", file_path)
        return exists

    return len(_list_root_csv_files(INPUT_FOLDER)) > 0


@dag(
    dag_id="realtime_ecommerce_processing",
    default_args={
        "owner": "data-team",
        "start_date": datetime(2026, 4, 29),
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    description="Real-Time E-Commerce Pipeline",
    schedule=None,
    catchup=False,
    tags=["realtime"],
)
def realtime_ecommerce_processing():
    # Wait until at least one eligible file is available.
    file_sensor = PythonSensor(
        task_id="wait_for_files",
        python_callable=_sensor_has_files,
        poke_interval=15,
        timeout=600,
        mode="reschedule",
    )

    @task(task_id="detect_files")
    def detect_files() -> List[Dict[str, str]]:
        # For watcher-triggered runs, process only the file passed via dag_run.conf.
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        input_file = conf.get("input_file")

        if input_file:
            target_path = os.path.join(INPUT_FOLDER, input_file)
            if not (os.path.exists(target_path) and os.path.dirname(os.path.abspath(target_path)) == os.path.abspath(INPUT_FOLDER.rstrip("/"))):
                raise AirflowException(f"Triggered input file not found in input root: {input_file}")
            files = [target_path]
            logger.info("Triggered run detected. Processing single file: %s", input_file)
        else:
            files = _list_root_csv_files(INPUT_FOLDER)
            logger.info("Manual run detected. Processing %s root csv file(s).", len(files))

        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        records = [
            {
                "file_path": file_path,
                "source_file": Path(file_path).name,
                "batch_id": batch_id,
            }
            for file_path in files
        ]
        logger.info("Detected %s files with batch_id: %s", len(records), batch_id)
        return records

    @task(task_id="read_data")
    def read_data(record: Dict[str, str]) -> Dict[str, Any]:
        import pandas as pd

        file_path = record["file_path"]
        try:
            df = RealtimeDataReader.read_csv(file_path)
            df["source_file"] = record["source_file"]
            raw_path = f"/tmp/raw_{Path(file_path).stem}_{datetime.now().timestamp()}.json"
            df.to_json(raw_path)
            return {**record, "data_path": raw_path, "row_count": len(df)}
        except Exception as exc:
            logger.warning("File failed during read_data: %s", record["source_file"])
            logger.exception("Read error for file %s", file_path)
            try:
                _move_to_failed(file_path)
            except Exception:
                logger.exception("Failed to move failed file %s", file_path)
            raise AirflowException(f"read_data failed for {record['source_file']}: {exc}")

    @task(task_id="validate_data")
    def validate_data(record: Dict[str, Any]) -> Dict[str, Any]:
        import pandas as pd

        try:
            df = pd.read_json(record["data_path"])
            report = RealtimeValidationRules.validate_all(df)

            checks = report.get("checks", {})
            critical_checks = ["mandatory_columns", "null_values", "date_validity"]
            critical_failed = [name for name in critical_checks if not checks.get(name, {}).get("passed", True)]

            non_critical_failed = [
                name
                for name in checks.keys()
                if name not in critical_checks and not checks.get(name, {}).get("passed", True)
            ]

            if non_critical_failed:
                logger.warning(
                    "Non-critical validation issues for %s. Continuing processing. Failed checks: %s",
                    record["source_file"],
                    non_critical_failed,
                )

            record = {**record, "validation_report": report}
            if critical_failed:
                raise AirflowException(
                    f"Critical validation failed for {record['source_file']}. Failed checks: {critical_failed}. Report: {report}"
                )
            return record
        except Exception as exc:
            logger.warning("File failed during validate_data: %s", record["source_file"])
            logger.exception("Validation error for file %s", record["source_file"])
            try:
                _move_to_failed(record["file_path"])
            except Exception:
                logger.exception("Failed to move failed file %s", record["file_path"])
            raise AirflowException(f"validate_data failed for {record['source_file']}: {exc}")

    @task(task_id="process_data")
    def process_data(record: Dict[str, Any]) -> Dict[str, Any]:
        import pandas as pd

        try:
            df = pd.read_json(record["data_path"])
            df_processed, _ = RealtimeDataProcessor.process_all(df)
            df_processed["revenue"] = df_processed.get("quantity", 0) * df_processed.get("unit_price", 0)
            df_processed["is_high_value"] = df_processed["revenue"] > 1000
            if "order_date" in df_processed.columns:
                df_processed["order_month"] = pd.to_datetime(df_processed["order_date"]).dt.month

            processed_path = f"/tmp/processed_{Path(record['file_path']).stem}_{datetime.now().timestamp()}.json"
            df_processed.to_json(processed_path)
            return {**record, "processed_path": processed_path, "processed_rows": len(df_processed)}
        except Exception as exc:
            logger.warning("File failed during process_data: %s", record["source_file"])
            logger.exception("Processing error for file %s", record["source_file"])
            try:
                _move_to_failed(record["file_path"])
            except Exception:
                logger.exception("Failed to move failed file %s", record["file_path"])
            raise AirflowException(f"process_data failed for {record['source_file']}: {exc}")

    @task(task_id="backup_validate")
    def backup_validate(record: Dict[str, Any]) -> Dict[str, Any]:
        import pandas as pd

        try:
            df = pd.read_json(record["processed_path"])
            duplicate_order_ids = int(df.duplicated(subset=["order_id"]).sum()) if "order_id" in df.columns else 0
            checks = [
                bool((df["quantity"] >= 0).all()) if "quantity" in df.columns else True,
                bool(df["order_id"].notnull().all()) if "order_id" in df.columns else True,
                bool((df["unit_price"] >= 0).all()) if "unit_price" in df.columns else True,
                bool((df["revenue"] >= 0).all()) if "revenue" in df.columns else True,
                bool(duplicate_order_ids == 0),
            ]
            check_names = [
                "quantity >= 0",
                "order_id not null",
                "unit_price >= 0",
                "revenue >= 0",
                "duplicate order_id removed",
            ]
            all_passed = bool(all(checks))
            logger.info("Backup Validation for %s:", record["source_file"])
            for name, passed in zip(check_names, checks):
                status = "PASS" if passed else "FAIL"
                logger.info("  %s %s", status, name)

            if not all_passed:
                raise AirflowException(f"Backup validation failed for {record['source_file']}")

            return {**record, "backup_report": {"passed": all_passed, "checks": dict(zip(check_names, checks))}}
        except Exception as exc:
            logger.warning("File failed during backup_validate: %s", record["source_file"])
            logger.exception("Backup validation error for file %s", record["source_file"])
            try:
                _move_to_failed(record["file_path"])
            except Exception:
                logger.exception("Failed to move failed file %s", record["file_path"])
            raise AirflowException(f"backup_validate failed for {record['source_file']}: {exc}")

    @task(task_id="write_data")
    def write_data(record: Dict[str, Any]) -> Dict[str, Any]:
        import pandas as pd

        file_path = record["file_path"]
        try:
            df = pd.read_json(record["processed_path"])
            logger.info("WRITE_DATA for %s (batch_id=%s)", record["source_file"], record["batch_id"])

            # Write to Azure only if a connection string is configured.
            if AZURE_CONNECTION_STRING:
                azure = AzureWriter(
                    connection_string=AZURE_CONNECTION_STRING,
                    container_name=AZURE_CONTAINER,
                )
                azure.write_both_to_azure(df, Path(record["source_file"]).stem, batch_timestamp=record["batch_id"])
            else:
                logger.warning("Azure connection string not configured; skipping Azure write for %s", record["source_file"])

            # Always write one local output file with output-prefix naming.
            os.makedirs(OUTPUT_REALTIME_FOLDER, exist_ok=True)
            local_out_path = os.path.join(OUTPUT_REALTIME_FOLDER, f"output{record['source_file']}")
            df.to_csv(local_out_path, index=False)
            logger.info("Wrote local output file: %s", local_out_path)

            return {**record, "local_output_path": local_out_path}
        except Exception as exc:
            logger.warning("File failed during write_data: %s", record["source_file"])
            logger.exception("Write error for file %s", record["source_file"])
            try:
                _move_to_failed(file_path)
            except Exception:
                logger.exception("Failed to move failed file %s", file_path)
            raise AirflowException(f"write_data failed for {record['source_file']}: {exc}")

    @task(task_id="move_files")
    def move_files(record: Dict[str, Any]) -> Dict[str, Any]:
        try:
            _move_to_succeeded(record["file_path"])
            return {**record, "moved": True}
        except Exception as exc:
            logger.warning("File failed during move_files: %s", record["source_file"])
            logger.exception("Move error for file %s", record["source_file"])
            try:
                _move_to_failed(record["file_path"])
            except Exception:
                logger.exception("Failed to move file to failed folder %s", record["file_path"])
            raise AirflowException(f"move_files failed for {record['source_file']}: {exc}")

    # Task chain per file record.
    records = detect_files()
    file_sensor >> records
    read_results = read_data.expand(record=records)
    validated_results = validate_data.expand(record=read_results)
    processed_results = process_data.expand(record=validated_results)
    backup_results = backup_validate.expand(record=processed_results)
    written_results = write_data.expand(record=backup_results)
    move_files.expand(record=written_results)


realtime_ecommerce_processing_dag = realtime_ecommerce_processing()
