import json
import os
from pathlib import Path

from fastapi import HTTPException


class PipelineService:

    @staticmethod
    def _get_status_path() -> Path:
        """
        Get the DE7 pipeline status file location.

        Can be overridden using PIPELINE_STATUS_PATH.
        Otherwise defaults to:
        <project_root>/data/pipeline_status/latest_status.json
        """

        configured_path = os.getenv("PIPELINE_STATUS_PATH")

        if configured_path:
            return Path(configured_path)

        # pipeline_service.py
        # project_root = D:\capstone1
        project_root = Path(__file__).resolve().parents[3]

        return (
            project_root
            / "data"
            / "pipeline_status"
            / "latest_status.json"
        )

    @staticmethod
    def get_status() -> dict:
        status_path = PipelineService._get_status_path()

        if not status_path.exists():
            raise FileNotFoundError(
                f"Pipeline status file not found: {status_path}"
            )

        try:
            with status_path.open(
                "r",
                encoding="utf-8",
            ) as file:
                status = json.load(file)

        except json.JSONDecodeError as exc:
            raise ValueError(
                "Pipeline status file contains invalid JSON"
            ) from exc

        except OSError as exc:
            raise OSError(
                f"Unable to read pipeline status file: {exc}"
            ) from exc

        # Basic validation of the machine-readable record.
        required_fields = [
            "run_id",
            "timestamp",
            "status",
            "tasks",
            "rows_in",
            "rows_rejected",
            "rows_published",
            "current_as_of",
            "freshness",
            "healthy",
            "reasons",
        ]

        missing_fields = [
            field
            for field in required_fields
            if field not in status
        ]

        if missing_fields:
            raise ValueError(
                "Pipeline status record is missing required fields: "
                + ", ".join(missing_fields)
            )

        return status