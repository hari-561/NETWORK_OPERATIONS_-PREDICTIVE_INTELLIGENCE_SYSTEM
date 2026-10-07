
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

WAREHOUSE_SCRIPT = (
    PROJECT_ROOT
    / "src"
    / "warehouse_loader.py"
)

LOG_DIR = (
    PROJECT_ROOT
    / "logs"
)

WAREHOUSE_LOG = (
    LOG_DIR
    / "warehouse_loader.log"
)


# ============================================================
# LOGGING FUNCTION
# ============================================================

def log_warehouse_start():

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        WAREHOUSE_LOG,
        "a",
        encoding="utf-8"
    ) as file:

        file.write(
            "\n"
            + "=" * 70
            + "\n"
        )

        file.write(
            f"WAREHOUSE LOAD START: "
            f"{datetime.now().isoformat()}\n"
        )


def log_warehouse_end():

    with open(
        WAREHOUSE_LOG,
        "a",
        encoding="utf-8"
    ) as file:

        file.write(
            f"WAREHOUSE LOAD END: "
            f"{datetime.now().isoformat()}\n"
        )

        file.write(
            "WAREHOUSE LOAD STATUS: SUCCESS\n"
        )


# ============================================================
# VALIDATION FUNCTION
# ============================================================

def validate_warehouse_inputs():

    print(
        "=" * 70
    )

    print(
        "VALIDATING WAREHOUSE INPUTS"
    )

    print(
        "=" * 70
    )

    # --------------------------------------------------------
    # Check warehouse loader script
    # --------------------------------------------------------

    if not WAREHOUSE_SCRIPT.exists():

        raise FileNotFoundError(
            "Warehouse loader script not found:\n"
            f"{WAREHOUSE_SCRIPT}"
        )

    print(
        f"Warehouse loader found: "
        f"{WAREHOUSE_SCRIPT}"
    )

    # --------------------------------------------------------
    # Check hourly grid summary
    # --------------------------------------------------------

    if not HOURLY_PATH.exists():

        raise FileNotFoundError(
            "Hourly grid summary Parquet not found:\n"
            f"{HOURLY_PATH}"
        )

    if not HOURLY_PATH.is_dir():

        raise ValueError(
            "Expected hourly_grid_summary_parquet "
            "to be a directory:\n"
            f"{HOURLY_PATH}"
        )

    parquet_files = list(
        HOURLY_PATH.rglob("*.parquet")
    )

    if not parquet_files:

        raise ValueError(
            "No Parquet files found inside:\n"
            f"{HOURLY_PATH}"
        )

    print(
        f"Hourly Parquet files found: "
        f"{len(parquet_files)}"
    )

    # --------------------------------------------------------
    # Check Milan grid reference
    # --------------------------------------------------------

    if not REFERENCE_PATH.exists():

        raise FileNotFoundError(
            "Milan grid reference not found:\n"
            f"{REFERENCE_PATH}"
        )

    print(
        f"GeoJSON reference found: "
        f"{REFERENCE_PATH}"
    )

    print(
        "=" * 70
    )

    print(
        "WAREHOUSE INPUT VALIDATION PASSED"
    )

    print(
        "=" * 70
    )


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id="telecom_warehouse_de6",

    description=(
        "DE6 Airflow to MySQL telecom warehouse "
        "loading pipeline"
    ),

    start_date=datetime(
        2026,
        9,
        2
    ),

    schedule=None,

    catchup=False,

    tags=[
        "network-project",
        "DE6",
        "warehouse",
        "mysql",
    ],

) as dag:

    # ========================================================
    # TASK 1 — LOG START
    # ========================================================

    warehouse_start = PythonOperator(

        task_id="warehouse_load_start",

        python_callable=log_warehouse_start,

    )

    # ========================================================
    # TASK 2 — VALIDATE INPUTS
    # ========================================================

    validate_inputs = PythonOperator(

        task_id="validate_warehouse_inputs",

        python_callable=validate_warehouse_inputs,

    )

    # ========================================================
    # TASK 3 — RUN WAREHOUSE LOADER
    # ========================================================

    run_warehouse = BashOperator(

        task_id="run_warehouse_loader",

        bash_command=(
            f"python3 {WAREHOUSE_SCRIPT} "
            f"--hourly {HOURLY_PATH} "
            f"--geojson {REFERENCE_PATH}"
        ),

        env={
            "PYTHONUNBUFFERED": "1"
        },

        append_env=True,

    )

    # ========================================================
    # TASK 4 — LOG SUCCESS
    # ========================================================

    warehouse_end = PythonOperator(

        task_id="warehouse_load_success_log",

        python_callable=log_warehouse_end,

    )

    # ========================================================
    # DEPENDENCIES
    # ========================================================

    (
        warehouse_start
        >> validate_inputs
        >> run_warehouse
        >> warehouse_end
    )

