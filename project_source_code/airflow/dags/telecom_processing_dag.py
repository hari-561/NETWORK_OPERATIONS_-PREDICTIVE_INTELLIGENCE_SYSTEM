from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.bash import BashOperator



# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_ROOT = Path(
    Path(__file__).resolve().parents[2]
)

RAW_PATH = PROJECT_ROOT / "data" / "raw"

PROCESSED_PATH = (
    PROJECT_ROOT / "data" / "processed"
)

ANALYTICS_PATH = (
    PROJECT_ROOT / "data" / "analytics"
)

REFERENCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "reference"
    / "milano-grid.geojson"
)

SPARK_SCRIPT = (
    PROJECT_ROOT
    / "src"
    / "telecom_pipeline.py"
)

LOG_DIR = (
    PROJECT_ROOT / "logs"
)

SPARK_LOG = (
    LOG_DIR / "spark_job.log"
)


# ============================================================
# LOGGING FUNCTION
# ============================================================

def log_spark_start():

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        SPARK_LOG,
        "a",
        encoding="utf-8"
    ) as file:

        file.write(
            "\n"
            + "=" * 70
            + "\n"
        )

        file.write(
            f"SPARK JOB START: "
            f"{datetime.now().isoformat()}\n"
        )


def log_spark_end():

    with open(
        SPARK_LOG,
        "a",
        encoding="utf-8"
    ) as file:

        file.write(
            f"SPARK JOB END: "
            f"{datetime.now().isoformat()}\n"
        )

        file.write(
            "SPARK JOB STATUS: SUCCESS\n"
        )


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="telecom_processing_de3",
    description=(
        "DE3 Airflow to Spark telecom processing pipeline"
    ),
    start_date=datetime(
        2026,
        8,
        30
    ),
    schedule=None,
    catchup=False,
    tags=[
        "network-project",
        "DE3",
        "spark",
    ],
) as dag:

    # ========================================================
    # TASK 1 — LOG START
    # ========================================================

    spark_start = PythonOperator(
        task_id="spark_job_start",
        python_callable=log_spark_start,
    )

    # ========================================================
    # TASK 2 — LAUNCH SPARK
    # ========================================================

    run_spark = BashOperator(
        task_id="run_spark_pipeline",

        bash_command=(
    f"python3 {SPARK_SCRIPT} "
    f"--input {RAW_PATH} "
    f"--output {PROCESSED_PATH} "
    f"--reference {REFERENCE_PATH} "
    f"--log {SPARK_LOG}"
),

        env={
            "PYTHONUNBUFFERED": "1"
        },

        append_env=True,
    )

    # ========================================================
    # TASK 3 — LOG SUCCESS
    # ========================================================

    spark_end = PythonOperator(
        task_id="spark_job_success_log",
        python_callable=log_spark_end,
    )

    # ========================================================
    # DEPENDENCIES
    # ========================================================

    spark_start >> run_spark >> spark_end