from pathlib import Path
from typing import Any

import joblib
import pandas as pd


class RiskService:
    """
    ML5 service responsible for operationalizing the trained
    ML3 high-activity risk model behind the FastAPI endpoint.
    """

    # MODEL_PATH = "D:/capstone1/ml/models/ml3_high_activity_decision_tree.joblib"

    MODEL_PATH = (
        Path(__file__).resolve().parents[3]
        / "ml"
        / "models"
        / "ml3_high_activity_decision_tree.joblib"
    )

    MODEL_FEATURES = [
        "avg_activity",
        "activity_growth",
        "peak_ratio",
        "variability",
        "internet_share",
        "trailing_median_24h",
        "hour_of_day",
        "day_of_week",
    ]

    MODEL_VERSION = "ml3-decision-tree-v1"

    _model = None

    @classmethod
    def _load_model(cls):
        """
        Load the trained ML3 model once.

        Raises:
            FileNotFoundError: if the model artifact does not exist.
            RuntimeError: if the artifact cannot be loaded.
        """
        model_path = Path(cls.MODEL_PATH)
        if cls._model is not None:
            return cls._model
        print("path ", model_path)

        if not model_path.exists():
            raise FileNotFoundError(
                "ML3 model artifact not found. "
                f"Expected model at: {model_path}"
            )

        try:
            cls._model = joblib.load(model_path)
        except Exception as exc:
            raise RuntimeError(
                "Failed to load the ML3 model artifact. "
                f"Path: {model_path}. "
                f"Error: {exc}"
            ) from exc

        return cls._model

    @classmethod
    def _validate_model(cls, model):
        """
        Validate that the loaded model exposes the expected
        prediction interface and feature information.
        """

        if not hasattr(model, "predict_proba"):
            raise RuntimeError(
                "Loaded ML3 model does not support predict_proba()."
            )

        if hasattr(model, "feature_names_in_"):
            actual_features = list(model.feature_names_in_)

            if actual_features != cls.MODEL_FEATURES:
                raise RuntimeError(
                    "ML3 model feature schema mismatch. "
                    f"Expected: {cls.MODEL_FEATURES}; "
                    f"Found: {actual_features}"
                )

    @classmethod
    def predict_risk(cls, request) -> dict[str, Any]:
        """
        Generate a real high-activity risk prediction.

        The risk score is the model probability for the positive
        ML3 target class:

            HIGH_ACTIVITY(t+1) = 1
        """

        model = cls._load_model()

        cls._validate_model(model)

        feature_data = {
            "avg_activity": request.avg_activity,
            "activity_growth": request.activity_growth,
            "peak_ratio": request.peak_ratio,
            "variability": request.variability,
            "internet_share": request.internet_share,
            "trailing_median_24h": request.trailing_median_24h,
            "hour_of_day": request.hour_of_day,
            "day_of_week": request.day_of_week,
        }

        missing_features = [
            feature
            for feature in cls.MODEL_FEATURES
            if feature not in feature_data
        ]

        if missing_features:
            raise ValueError(
                "Missing required ML3 model features: "
                f"{missing_features}"
            )

        feature_row = pd.DataFrame(
            [[feature_data[feature] for feature in cls.MODEL_FEATURES]],
            columns=cls.MODEL_FEATURES,
        )

        probabilities = model.predict_proba(feature_row)

        risk_score = float(probabilities[0, 1])

        if risk_score >= 0.80:
            risk_level = "CRITICAL"
        elif risk_score >= 0.60:
            risk_level = "HIGH"
        elif risk_score >= 0.30:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"

        explanation_note = (
            f"Predicted high-activity risk is {risk_score:.1%}. "
            "The prediction represents the model's estimated "
            "probability of the defined ML3 high-activity target "
            "for the next hourly interval. "
            "It is an operational risk indicator and does not "
            "confirm network congestion."
        )

        return {
            "grid_id": request.grid_id,
            "feature_timestamp": request.feature_timestamp,
            "risk_score": risk_score,
            "risk_level": risk_level,
            "model_version": cls.MODEL_VERSION,
            "explanation_note": explanation_note,
        }