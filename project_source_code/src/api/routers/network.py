from datetime import datetime,date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from database import get_db
from models.network import (
    GridFeaturesResponse2,
    NetworkSummaryResponse,
    GridActivityResponse,
    AlertResponse,
    GridFeaturesResponse,
    RiskPredictionRequest,
    RiskPredictionResponse,
    GridLocationResponse,
    AnomalyResponse,
    ClaudeAssistantRequest,
    ClaudeAssistantResponse,
)
from services.network_service import NetworkService
from services.alert_service import AlertService
from services.risk_service import RiskService


router = APIRouter(
    prefix="/network",
    tags=["Network"],
)


@router.get(
    "/summary",
    response_model=NetworkSummaryResponse,
)
def get_network_summary(
    as_of: datetime | None = Query(
        default=None,
        description="Analytics timestamp to evaluate",
    ),
    db: Session = Depends(get_db),
):
    try:

        return NetworkService.get_summary(
            db=db,
            as_of=as_of,
        )

    except SQLAlchemyError as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Analytics database unavailable: "
                f"{str(exc)}"
            ),
        )

    except ValueError as exc:

        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to retrieve network summary: "
                f"{str(exc)}"
            ),
        )


@router.get(
    "/grid/{grid_id}",
    response_model=GridActivityResponse,
)
def get_grid_activity(
    grid_id: int,

    date: date | None = Query(
        default=None,
        description="Filter by calendar date",
    ),

    hour: int | None = Query(
        default=None,
        ge=0,
        le=23,
        description="Filter by hour of day",
    ),

    as_of: datetime | None = Query(
        default=None,
        description="Effective analytics timestamp",
    ),

    db: Session = Depends(get_db),
):
    try:

        return NetworkService.get_grid_activity(
            db=db,
            grid_id=grid_id,
            date=(
                date.isoformat()
                if date is not None
                else None
            ),
            hour=hour,
            as_of=as_of,
        )

    except LookupError as exc:

        raise HTTPException(
            status_code=404,
            detail=str(exc),
        )

    except SQLAlchemyError as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Analytics database unavailable: "
                f"{exc}"
            ),
        )

    except ValueError as exc:

        raise HTTPException(
            status_code=404,
            detail=str(exc),
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to retrieve grid activity: "
                f"{exc}"
            ),
        )




@router.get(
    "/alerts",
    response_model=AlertResponse,
)
def get_network_alerts(
    limit: int = Query(
        default=100,
        ge=1,
        le=100000,
        description="Maximum number of alerts",
    ),

    severity: str | None = Query(
        default=None,
        description="Filter by severity",
    ),

    as_of: datetime | None = Query(
        default=None,
        description="Effective analytics timestamp",
    ),

    db: Session = Depends(get_db),
):

    try:

        return AlertService.get_alerts(
            db=db,
            limit=limit,
            severity=severity,
            as_of=as_of,
        )

    except SQLAlchemyError as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Analytics database unavailable: "
                f"{exc}"
            ),
        )

    except ValueError as exc:

        raise HTTPException(
            status_code=400,
            detail=str(exc),
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to retrieve network alerts: "
                f"{exc}"
            ),
        )
@router.get(
    "/hotspots",
    response_model=AlertResponse,
)
def get_network_hotspots(
    limit: int = Query(
        default=100,
        ge=1,
        le=1000,
        description="Maximum number of hotspots",
    ),

    severity: str | None = Query(
        default=None,
        description="Filter hotspots by severity",
    ),

    as_of: datetime | None = Query(
        default=None,
        description="Effective analytics timestamp",
    ),

    db: Session = Depends(get_db),
):

    try:

        return AlertService.get_hotspots(
            db=db,
            limit=limit,
            severity=severity,
            as_of=as_of,
        )

    except SQLAlchemyError as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Analytics database unavailable: "
                f"{exc}"
            ),
        )

    except ValueError as exc:

        raise HTTPException(
            status_code=400,
            detail=str(exc),
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to retrieve hotspots: "
                f"{exc}"
            ),
        )

from datetime import datetime


@router.get(
    "/grid/{grid_id}/features",
    response_model=GridFeaturesResponse,
)
def get_grid_features(
    grid_id: int,
    as_of: datetime | None = None,
    db: Session = Depends(get_db),
):
    try:

        return NetworkService.get_grid_features(
            db=db,
            grid_id=grid_id,
            as_of=as_of,
        )

    except LookupError as exc:

        raise HTTPException(
            status_code=404,
            detail=str(exc),
        )

    except SQLAlchemyError as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Feature database unavailable: "
                f"{exc}"
            ),
        )

    except ValueError as exc:

        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to retrieve grid features: "
                f"{exc}"
            ),
        )


@router.get(
    "/grid/{grid_id}/features2",
    response_model=GridFeaturesResponse2,
)
def get_grid_features2(
    grid_id: int,
    as_of: datetime | None = None,
    db: Session = Depends(get_db),
):
    try:

        return NetworkService.get_grid_features2(
            db=db,
            grid_id=grid_id,
            as_of=as_of,
        )

    except LookupError as exc:

        raise HTTPException(
            status_code=404,
            detail=str(exc),
        )

    except SQLAlchemyError as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Feature database unavailable: "
                f"{exc}"
            ),
        )

    except ValueError as exc:

        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to retrieve grid features: "
                f"{exc}"
            ),
        )

@router.post(
    "/predict-risk",
    response_model=RiskPredictionResponse,
)
def predict_network_risk(
    request: RiskPredictionRequest,
):
    try:

        return RiskService.predict_risk(
            request
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Risk prediction failed: "
                f"{exc}"
            ),
        )


@router.get(
    "/grid/{grid_id}/location",
    response_model=GridLocationResponse,
)
def get_grid_location(
    grid_id: int,
    db: Session = Depends(get_db),
):
    try:

        return NetworkService.get_grid_location(
            db=db,
            grid_id=grid_id,
        )

    except LookupError as exc:

        raise HTTPException(
            status_code=404,
            detail=str(exc),
        )

    except SQLAlchemyError as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Grid location database unavailable: "
                f"{exc}"
            ),
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to retrieve grid location: "
                f"{exc}"
            ),
        )



@router.get(
    "/grid/{grid_id}/anomaly",
    response_model=AnomalyResponse,
)
def get_grid_anomaly(
    grid_id: int,
    as_of: datetime | None = Query(
        default=None,
        description="Effective analytics timestamp",
    ),
    db: Session = Depends(get_db),
):
    try:

        return NetworkService.get_anomaly_score(
            db=db,
            grid_id=grid_id,
            as_of=as_of,
        )

    except LookupError as exc:

        raise HTTPException(
            status_code=404,
            detail=str(exc),
        )

    except SQLAlchemyError as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Anomaly database unavailable: "
                f"{exc}"
            ),
        )

    except ValueError as exc:

        raise HTTPException(
            status_code=400,
            detail=str(exc),
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to retrieve anomaly data: "
                f"{exc}"
            ),
        )

