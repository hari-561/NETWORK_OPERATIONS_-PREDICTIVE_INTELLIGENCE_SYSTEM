from fastapi import APIRouter, HTTPException

from models.network import PipelineStatusResponse
from services.pipeline_service import PipelineService


router = APIRouter(
    prefix="/pipeline",
    tags=["Pipeline"],
)


@router.get(
    "/status",
    response_model=PipelineStatusResponse,
)
def get_pipeline_status():

    try:
        return PipelineService.get_status()

    except FileNotFoundError as exc:

        raise HTTPException(
            status_code=503,
            detail=str(exc),
        )

    except ValueError as exc:

        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )

    except OSError as exc:

        raise HTTPException(
            status_code=503,
            detail=str(exc),
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to retrieve pipeline status: "
                f"{exc}"
            ),
        )