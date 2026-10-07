from datetime import datetime

from airflow import DAG
from airflow.sdk import task


# ============================================================
# PROJECT PATH
# ============================================================

from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]

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
# DAG
# ============================================================

with DAG(
    dag_id="telecom_ingestion_de2",
    description="DE2 telecom daily CSV ingestion pipeline",
    start_date=datetime(2026, 8, 28),
    schedule=None,
    catchup=False,
    tags=["network-project", "DE2", "ingestion"],
) as dag:

    # ========================================================
    # TASK 1 — DETECT
    # ========================================================

    @task
    def detect():

        files = detect_files()

        file_paths = [
            str(file)
            for file in files
        ]

        print(
            f"Detected {len(file_paths)} files."
        )

        return file_paths

    # ========================================================
    # TASK 2 — VALIDATE
    # ========================================================

    @task
    def validate(file_paths):

        validation_results = []

        for file_path_string in file_paths:

            file_path = Path(file_path_string)

            print(
                f"\nValidating: {file_path.name}"
            )

            schema_result = validate_schema(
                file_path
            )

            # ----------------------------------------------
            # Schema failed
            # ----------------------------------------------

            if not schema_result["valid"]:

                validation_results.append(
                    {
                        "file_path": str(file_path),
                        "valid": False,
                        "row_count": schema_result[
                            "row_count"
                        ],
                        "reason": schema_result[
                            "reason"
                        ],
                    }
                )

                continue

            # ----------------------------------------------
            # Minimum quality validation
            # ----------------------------------------------

            quality_result = (
                validate_minimum_quality(
                    file_path
                )
            )

            validation_results.append(
                {
                    "file_path": str(file_path),
                    "valid": quality_result[
                        "valid"
                    ],
                    "row_count": quality_result[
                        "row_count"
                    ],
                    "reason": quality_result[
                        "reason"
                    ],
                }
            )

        return validation_results

    # ========================================================
    # TASK 3 — ROUTE
    # ========================================================

    @task
    def route(validation_results):

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

            routed["row_count"] = result[
                "row_count"
            ]

            routing_results.append(routed)

        return routing_results

    # ========================================================
    # TASK 4 — LOG
    # ========================================================

    @task
    def log(routing_results):

        for result in routing_results:

            write_ingestion_metadata(
                filename=result["filename"],
                status=result["status"],
                row_count=result["row_count"],
                reason=result["reason"],
            )

        print(
            "Ingestion metadata written successfully."
        )

    # ========================================================
    # TASK DEPENDENCIES
    # ========================================================

    detected_files = detect()

    validation_results = validate(
        detected_files
    )

    routing_results = route(
        validation_results
    )

    log(
        routing_results
    )