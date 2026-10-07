
from datetime import datetime
from pathlib import Path
import json
import subprocess
import sys


from airflow import DAG
from airflow.sdk import task,get_current_context
from airflow.sdk import get_current_context, TaskInstance




# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_ROOT = Path(
    Path(__file__).resolve().parents[2]
)


# ============================================================
# INPUT / OUTPUT PATHS
# ============================================================

LANDING_PATH = (
    PROJECT_ROOT
    / "data"
    / "landing"
)

RAW_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
)

REJECTED_PATH = (
    PROJECT_ROOT
    / "data"
    / "rejected"
)

PROCESSED_PATH = (
    PROJECT_ROOT
    / "data"
    / "processed"
)

HOURLY_PATH = (
    PROCESSED_PATH
    / "hourly_grid_summary_parquet"
)

REFERENCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "reference"
    / "milano-grid.geojson"
)


# ============================================================
# SCRIPT PATHS
# ============================================================

TELECOM_PIPELINE = (
    PROJECT_ROOT
    / "src"
    / "telecom_pipeline.py"
)

WAREHOUSE_LOADER = (
    PROJECT_ROOT
    / "src"
    / "warehouse_loader.py"
)


# ============================================================
# PIPELINE STATUS
# ============================================================

STATUS_DIR = (
    PROJECT_ROOT
    / "data"
    / "pipeline_status"
)

STATUS_FILE = (
    STATUS_DIR
    / "latest_status.json"
)


# ============================================================
# LOGGING
# ============================================================

LOG_DIR = (
    PROJECT_ROOT
    / "logs"
)

PIPELINE_LOG = (
    LOG_DIR
    / "end_to_end_pipeline.log"
)


# ============================================================
# IMPORT EXISTING INGESTION FUNCTIONS
# ============================================================

sys.path.insert(
    0,
    str(PROJECT_ROOT)
)


from src.ingestion import (
    detect_files,
    validate_schema,
    validate_minimum_quality,
    route_file,
    write_ingestion_metadata,
)


# ============================================================
# HELPER — WRITE PIPELINE STATUS
# ============================================================

def write_status(status):

    STATUS_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        STATUS_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            status,
            file,
            indent=4,
            default=str
        )


# ============================================================
# HELPER — LOG PIPELINE EVENT
# ============================================================

def log_pipeline_event(message):

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        PIPELINE_LOG,
        "a",
        encoding="utf-8"
    ) as file:

        file.write(
            f"{datetime.now().isoformat()} | "
            f"{message}\n"
        )


# ============================================================
# HELPER — GET AIRFLOW TASK STATUS
# ============================================================



def get_task_status(task_id):
    context = get_current_context()
    dag_run = context["dag_run"]

    try:
        task_states = TaskInstance.get_task_states(
            dag_id=dag_run.dag_id,
            task_ids=[task_id],
            run_ids=[dag_run.run_id],
        )

        state = task_states.get(task_id)

        if state is None:
            return "UNKNOWN"

        return str(state).upper()

    except Exception as e:
        print(f"Could not get status for task {task_id}: {e}")
        return "UNKNOWN"
    
# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id="telecom_end_to_end_de7",

    description=(
        "DE7 end-to-end telecom orchestration pipeline"
    ),

    start_date=datetime(
        2026,
        9,
        3
    ),

    schedule=None,

    catchup=False,

    tags=[
        "network-project",
        "DE7",
        "orchestration",
        "end-to-end",
    ],

) as dag:

    # ========================================================
    # TASK 1 — INGEST
    # ========================================================

    @task
    def ingest():

        print(
            "=" * 70
        )

        print(
            "TASK: INGEST"
        )

        print(
            "=" * 70
        )

        files = detect_files()

        if not files:

            raise ValueError(
                "No telecom CSV files detected "
                "in landing area."
            )

        file_paths = [
            str(file)
            for file in files
        ]

        print(
            f"Detected {len(file_paths)} files."
        )

        for file_path in file_paths:

            print(
                f"Detected file: {file_path}"
            )

        return file_paths


    # ========================================================
    # TASK 2 — VALIDATE
    # ========================================================

    @task
    def validate(file_paths):

        print(
            "=" * 70
        )

        print(
            "TASK: VALIDATE"
        )

        print(
            "=" * 70
        )

        validation_results = []

        for file_path_string in file_paths:

            file_path = Path(
                file_path_string
            )

            print(
                f"\nValidating: "
                f"{file_path.name}"
            )

            schema_result = validate_schema(
                file_path
            )

            if not schema_result["valid"]:

                validation_results.append(
                    {
                        "file_path":
                            str(file_path),

                        "valid":
                            False,

                        "row_count":
                            schema_result[
                                "row_count"
                            ],

                        "reason":
                            schema_result[
                                "reason"
                            ],
                    }
                )

                continue

            quality_result = (
                validate_minimum_quality(
                    file_path
                )
            )

            validation_results.append(
                {
                    "file_path":
                        str(file_path),

                    "valid":
                        quality_result[
                            "valid"
                        ],

                    "row_count":
                        quality_result[
                            "row_count"
                        ],

                    "reason":
                        quality_result[
                            "reason"
                        ],
                }
            )

        invalid_files = [
            result
            for result in validation_results
            if not result["valid"]
        ]

        if invalid_files:

            print(
                f"Validation rejected "
                f"{len(invalid_files)} file(s)."
            )

        return validation_results


    # ========================================================
    # TASK 3 — ROUTE
    # ========================================================

    @task
    def route(validation_results):

        print(
            "=" * 70
        )

        print(
            "TASK: ROUTE"
        )

        print(
            "=" * 70
        )

        routing_results = []

        for result in validation_results:

            file_path = Path(
                result["file_path"]
            )

            routed = route_file(
                file_path=file_path,

                valid=result["valid"],

                reason=result["reason"],
            )

            routed["row_count"] = (
                result["row_count"]
            )

            routing_results.append(
                routed
            )

        # ----------------------------------------------------
        # Write ingestion metadata
        # ----------------------------------------------------

        for result in routing_results:

            write_ingestion_metadata(
                filename=result[
                    "filename"
                ],

                status=result[
                    "status"
                ],

                row_count=result[
                    "row_count"
                ],

                reason=result[
                    "reason"
                ],
            )

        successful_routes = [
            result
            for result in routing_results
            if result["status"] == "ACCEPTED"
        ]

        if not successful_routes:

            raise ValueError(
                "No valid files were routed "
                "to the raw layer."
            )

        print(
            f"Successfully routed "
            f"{len(successful_routes)} file(s)."
        )

        return routing_results


    # ========================================================
    # TASK 4 — SPARK PROCESS
    # ========================================================

    @task
    def spark_process(routing_results):

        print(
            "=" * 70
        )

        print(
            "TASK: SPARK PROCESS"
        )

        print(
            "=" * 70
        )

        if not TELECOM_PIPELINE.exists():

            raise FileNotFoundError(
                "telecom_pipeline.py not found:\n"
                f"{TELECOM_PIPELINE}"
            )

        RAW_PATH.mkdir(
            parents=True,
            exist_ok=True
        )

        PROCESSED_PATH.mkdir(
            parents=True,
            exist_ok=True
        )

        LOG_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        command = [
            sys.executable,

            str(TELECOM_PIPELINE),

            "--input",
            str(RAW_PATH),

            "--output",
            str(PROCESSED_PATH),

            "--reference",
            str(REFERENCE_PATH),

            "--log",
            str(PIPELINE_LOG),
        ]

        print(
            "Running Spark pipeline:"
        )

        print(
            " ".join(command)
        )

        result = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            check=False,
        )

        if result.returncode != 0:

            raise RuntimeError(
                "telecom_pipeline.py failed "
                f"with exit code "
                f"{result.returncode}"
            )

        if not HOURLY_PATH.exists():

            raise FileNotFoundError(
                "Spark processing completed but "
                "hourly_grid_summary_parquet "
                "was not created:\n"
                f"{HOURLY_PATH}"
            )

        print(
            "Spark processing completed successfully."
        )

        return {
            "status": "SUCCESS",
            "hourly_path": str(HOURLY_PATH),
        }


    # ========================================================
    # TASK 5 — LOAD WAREHOUSE
    # ========================================================

    @task
    def load_warehouse(spark_result):

        print(
            "=" * 70
        )

        print(
            "TASK: LOAD WAREHOUSE"
        )

        print(
            "=" * 70
        )

        if not WAREHOUSE_LOADER.exists():

            raise FileNotFoundError(
                "warehouse_loader.py not found:\n"
                f"{WAREHOUSE_LOADER}"
            )

        if not HOURLY_PATH.exists():

            raise FileNotFoundError(
                "Hourly Spark output not found:\n"
                f"{HOURLY_PATH}"
            )

        if not REFERENCE_PATH.exists():

            raise FileNotFoundError(
                "GeoJSON reference not found:\n"
                f"{REFERENCE_PATH}"
            )

        command = [
            sys.executable,

            str(WAREHOUSE_LOADER),

            "--hourly",
            str(HOURLY_PATH),

            "--geojson",
            str(REFERENCE_PATH),
        ]

        print(
            "Running warehouse loader:"
        )

        print(
            " ".join(command)
        )

        result = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            check=False,
        )

        if result.returncode != 0:

            raise RuntimeError(
                "warehouse_loader.py failed "
                f"with exit code "
                f"{result.returncode}"
            )

        print(
            "Warehouse loading completed successfully."
        )

        return {
            "status": "SUCCESS"
        }


    # ========================================================
    # TASK 6 — QUALITY CHECK
    #
    # Writes the machine-readable JSON consumed by API6.
    # ========================================================

    @task
    def quality_check(
        file_paths,
        validation_results,
        routing_results,
        spark_result,
        warehouse_result,
        **context,
    ):

        print(
            "=" * 70
        )

        print(
            "TASK: QUALITY CHECK"
        )

        print(
            "=" * 70
        )

        # ----------------------------------------------------
        # Rows IN
        # ----------------------------------------------------

        rows_in = sum(
            result["row_count"]
            for result in validation_results
        )

        # ----------------------------------------------------
        # Rows REJECTED
        # ----------------------------------------------------

        rows_rejected = sum(
            result["row_count"]
            for result in validation_results
            if not result["valid"]
        )

        # ----------------------------------------------------
        # Published Parquet files
        # ----------------------------------------------------

        published_files = list(
            HOURLY_PATH.rglob(
                "*.parquet"
            )
        )

        if not published_files:

            raise ValueError(
                "Quality check failed: "
                "no published Parquet files found."
            )

        # ----------------------------------------------------
        # Read final analytics layer
        # ----------------------------------------------------

        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder
            .appName(
                "TelecomQualityCheck"
            )
            .getOrCreate()
        )

        try:

            final_df = (
                spark.read
                .parquet(
                    str(HOURLY_PATH)
                )
            )

            rows_published = (
                final_df.count()
            )

            current_as_of = None

            if "timestamp" in final_df.columns:

                current_as_of = (
                    final_df
                    .selectExpr(
                        "max(timestamp) "
                        "as max_timestamp"
                    )
                    .collect()[0]
                    ["max_timestamp"]
                )

        finally:

            spark.stop()

        # ----------------------------------------------------
        # Warehouse result
        # ----------------------------------------------------

        spark_status = (
            spark_result["status"]
        )

        warehouse_status = (
            warehouse_result["status"]
        )

        # ----------------------------------------------------
        # Task statuses
        # ----------------------------------------------------

        task_status = {

    "ingest": get_task_status("ingest"),

    "validate": get_task_status("validate"),

    "route": get_task_status("route"),

    "spark_process": spark_status,

    "load_warehouse": warehouse_status,

    "quality_check": "SUCCESS",
}
        # ----------------------------------------------------
        # Quality result
        # ----------------------------------------------------

        reasons = []

        healthy = True

        if rows_published == 0:

            healthy = False

            reasons.append(
                "Analytics layer contains zero rows."
            )

        if spark_status != "SUCCESS":

            healthy = False

            reasons.append(
                "Spark processing was not successful."
            )

        if warehouse_status != "SUCCESS":

            healthy = False

            reasons.append(
                "Warehouse loading was not successful."
            )

        # ----------------------------------------------------
        # Freshness
        # ----------------------------------------------------

        freshness_indicator = "UNKNOWN"
        freshness_age_hours = None

        if current_as_of is not None:

            if isinstance(
                current_as_of,
                datetime
            ):

                data_timestamp = (
                    current_as_of
                )

            else:

                data_timestamp = (
                    datetime.fromisoformat(
                        str(current_as_of)
                    )
                )

            current_time = datetime.now()

            freshness_age_hours = (
                current_time
                - data_timestamp
            ).total_seconds() / 3600

            if freshness_age_hours <= 2:

                freshness_indicator = "FRESH"

            elif freshness_age_hours <= 24:

                freshness_indicator = "RECENT"

            else:

                freshness_indicator = "STALE"

        # ----------------------------------------------------
        # Machine-readable status record
        # ----------------------------------------------------

        status = {

            "run_id":
                context["dag_run"].run_id,

            "timestamp":
                datetime.now().isoformat(),

            "status":
                "SUCCESS"
                if healthy
                else "FAILED",

            "tasks":
                task_status,

            "rows_in":
                rows_in,

            "rows_rejected":
                rows_rejected,

            "nulls_handled":
                None,

            "rows_published":
                rows_published,

            "current_as_of":
                current_as_of,

            "freshness": {

                "latest_data_timestamp":
                    current_as_of,

                "age_hours":
                    freshness_age_hours,

                "indicator":
                    freshness_indicator,
            },

            "healthy":
                healthy,

            "reasons":
                reasons,
        }

        # ----------------------------------------------------
        # Write JSON status
        # ----------------------------------------------------

        write_status(
            status
        )

        print(
            "Machine-readable pipeline status "
            "written successfully."
        )

        print(
            f"Status file:\n{STATUS_FILE}"
        )

        print(
            json.dumps(
                status,
                indent=4,
                default=str
            )
        )

        # ----------------------------------------------------
        # Human-readable log
        # ----------------------------------------------------

        log_pipeline_event(
            (
                f"DE7 STATUS: "
                f"{status['status']} | "
                f"RUN_ID: "
                f"{status['run_id']} | "
                f"AS_OF: "
                f"{status['current_as_of']} | "
                f"FRESHNESS: "
                f"{freshness_indicator}"
            )
        )

        if not healthy:

            raise ValueError(
                "Pipeline quality check failed."
            )

        return status


    # ========================================================
    # TASK 7 — NOTIFY
    # ========================================================

    @task
    def notify(status):

        print(
            "=" * 70
        )

        print(
            "TASK: NOTIFY"
        )

        print(
            "=" * 70
        )

        log_pipeline_event(
            (
                "END-TO-END PIPELINE COMPLETED | "
                f"STATUS: {status['status']} | "
                f"HEALTHY: {status['healthy']}"
            )
        )

        if status["healthy"]:

            print(
                "END-TO-END PIPELINE "
                "COMPLETED SUCCESSFULLY."
            )

        else:

            print(
                "END-TO-END PIPELINE "
                "FAILED."
            )


    # ========================================================
    # TASK DEPENDENCIES
    # ========================================================

    detected_files = ingest()

    validation_results = validate(
        detected_files
    )

    routing_results = route(
        validation_results
    )

    spark_result = spark_process(
        routing_results
    )

    warehouse_result = load_warehouse(
        spark_result
    )

    status = quality_check(
        detected_files,
        validation_results,
        routing_results,
        spark_result,
        warehouse_result,
    )

    notify(
        status
    )

