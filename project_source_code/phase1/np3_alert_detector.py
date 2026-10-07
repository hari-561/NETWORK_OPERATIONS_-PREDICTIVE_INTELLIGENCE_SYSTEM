import logging
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# Logging
# ============================================================


log_directory = Path("../log")

log_directory.mkdir(
    parents=True,
    exist_ok=True
)

log_file = log_directory / "np3.log"

logger = logging.getLogger(__name__)

# if not logger.handlers:
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
#     )


logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("NP3")


# ============================================================
# RuleBasedAlertDetector
# ============================================================


class RuleBasedAlertDetector:

    REQUIRED_COLUMNS = [
        "timestamp",
        "grid_id",
        "total_activity"
    ]
    

    def __init__(
        self,
        dataframe=None,
        file_path=None,
        activity_floor=None,
        high_multiplier=1.50,
        drop_multiplier=0.50,
        spike_multiplier=1.50
    ):

        if dataframe is None and file_path is None:
            raise ValueError(
                "Provide either dataframe or file_path."
            )

        if dataframe is not None and file_path is not None:
            raise ValueError(
                "Provide either dataframe or file_path, not both."
            )

        self.dataframe = (
            dataframe.copy()
            if dataframe is not None
            else None
        )

        self.file_path = (
            Path(file_path)
            if file_path is not None
            else None
        )

        # Rule thresholds
        self.high_multiplier = high_multiplier
        self.drop_multiplier = drop_multiplier
        self.spike_multiplier = spike_multiplier

        # Activity floor
        self.activity_floor = activity_floor

        # Outputs
        self.data = None
        self.alerts = None
        self.summary = None

    # ========================================================
    # 1. Load analytics data
    # ========================================================

    def load_data(self):

        if self.dataframe is not None:

            self.data = self.dataframe.copy()

            logger.info(
                "Loaded grid/hour analytics from DataFrame: %d rows",
                len(self.data)
            )

        else:

            if not self.file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {self.file_path}"
                )

            self.data = pd.read_csv(
                self.file_path
            )

            logger.info(
                "Loaded grid/hour analytics from %s: %d rows",
                self.file_path,
                len(self.data)
            )

        return self.data

    # ========================================================
    # 2. Validate input
    # ========================================================

    def validate_input(self):

        if self.data is None:
            raise RuntimeError(
                "Call load_data() first."
            )

        missing = [
            col
            for col in self.REQUIRED_COLUMNS
            if col not in self.data.columns
        ]

        if missing:
            raise ValueError(
                f"Missing required columns: {missing}"
            )

       
        # Timestamp
        self.data["timestamp"] = pd.to_datetime(
            self.data["timestamp"],
            errors="coerce"
        )

        # Activity
        self.data["total_activity"] = pd.to_numeric(
            self.data["total_activity"],
            errors="coerce"
        )

        # Structural fields cannot be missing
        invalid = (
            self.data["timestamp"].isna()
            | self.data["grid_id"].isna()
            | self.data["total_activity"].isna()
        )

        invalid_count = invalid.sum()

       
        if invalid_count > 0:

            logger.warning(
                "Dropping %d invalid grid/hour rows",
                invalid_count
            )

            self.data = self.data.loc[
                ~invalid
            ].copy()

        # Negative activity should never exist
        negative_count = (
            self.data["total_activity"] < 0
        ).sum()

        if negative_count > 0:

            raise ValueError(
                f"Found {negative_count} negative "
                "total_activity values."
            )

        # Ensure sorting
        self.data = self.data.sort_values(
            ["grid_id", "timestamp"]
        ).reset_index(drop=True)

        logger.info(
            "Input validation complete: %d rows",
            len(self.data)
        )

        return self.data

    # ========================================================
    # 3. Choose activity floor
    # ========================================================

    def choose_activity_floor(self):

        if self.activity_floor is not None:

            logger.info(
                "Using configured activity floor: %.2f",
                self.activity_floor
            )

            return self.activity_floor

        # ----------------------------------------------------
        # Calculate total daily activity for each grid
        # ----------------------------------------------------

        daily_grid_activity = (
            self.data
            .groupby(
                ["grid_id", self.data["timestamp"].dt.date]
            )["total_activity"]
            .sum()
        )

        # ----------------------------------------------------
        # Data-driven floor
        #
        # We use the 10th percentile of daily grid activity.
        #
        # This removes the lowest-activity 10% of grid-days
        # from ratio-based alerting.
        # ----------------------------------------------------

        floor = float(
            daily_grid_activity.quantile(0.10)
        )

        self.activity_floor = floor

        logger.info(
            "Data-driven activity floor selected: %.4f "
            "(10th percentile of daily grid activity)",
            floor
        )

        return floor

    # ========================================================
    # 4. Build within-day baseline
    # ========================================================

    def build_baseline(self):

        if self.data is None:
            raise RuntimeError(
                "Load and validate data first."
            )

        # Date column
        self.data["date"] = (
            self.data["timestamp"].dt.date
        )

        # Hour
        self.data["hour"] = (
            self.data["timestamp"].dt.hour
        )

        # ----------------------------------------------------
        # Daily total
        # ----------------------------------------------------

        daily_total = (
            self.data
            .groupby(
                ["grid_id", "date"]
            )["total_activity"]
            .transform("sum")
        )

        self.data["daily_total_activity"] = daily_total

        # ----------------------------------------------------
        # Calculate baseline excluding current hour
        #
        # For each grid/day:
        #
        # baseline =
        # median(
        #   all other hours
        # )
        #
        # We use:
        #
        # (daily_sum - current_activity)
        # /
        # (number_of_hours - 1)
        #
        # only as a helper to remove current hour.
        #
        # Because median cannot be reconstructed from sums,
        # we calculate the leave-one-out median explicitly.
        # ----------------------------------------------------

        grouped = (
            self.data
            .groupby(
                ["grid_id", "date"]
            )
        )

        baselines = []

        for (grid_id, date), group in grouped:

            values = (
                group["total_activity"]
                .to_numpy()
            )

            indices = group.index

            for idx in indices:

                current_value = (
                    self.data.loc[
                        idx,
                        "total_activity"
                    ]
                )

                other_values = values[
                    values != current_value
                ]

                # IMPORTANT:
                # The above is not sufficient when duplicate
                # activity values exist.
                #
                # Therefore reconstruct using positional
                # index instead.

                position = list(indices).index(idx)

                other_values = np.delete(
                    values,
                    position
                )

                if len(other_values) == 0:
                    baseline = np.nan
                else:
                    baseline = float(
                        np.median(other_values)
                    )

                baselines.append(
                    (
                        idx,
                        baseline
                    )
                )

        baseline_series = pd.Series(
            dict(baselines)
        )

        self.data["baseline_activity"] = (
            baseline_series
        )

        logger.info(
            "Within-day leave-one-out median baseline created."
        )

        return self.data

    # ========================================================
    # 5. Apply activity floor
    # ========================================================

    def validate_daily_coverage(self):

        hourly_counts = (
            self.data
            .groupby(
                ["grid_id", "date"]
            )["hour"]
            .nunique()
        )

        incomplete_days = hourly_counts[
            hourly_counts < 24
        ]

        logger.info(
            "Grid-days with complete 24-hour coverage: %d",
            (hourly_counts == 24).sum()
        )

        logger.warning(
            "Grid-days with incomplete coverage: %d",
            len(incomplete_days)
        )

        return incomplete_days



    def apply_activity_floor(self):

        if "daily_total_activity" not in self.data.columns:
            raise RuntimeError(
                "Build baseline before applying activity floor."
            )

        self.data["eligible_for_alert"] = (
            self.data["daily_total_activity"]
            >= self.activity_floor
        )

        excluded = (
            ~self.data["eligible_for_alert"]
        ).sum()

        logger.info(
            "Excluded %d grid/hour rows below activity floor %.4f",
            excluded,
            self.activity_floor
        )

        return self.data

    # ========================================================
    # 6. Apply alert rules
    # ========================================================

    def apply_rules(self):

        if "baseline_activity" not in self.data.columns:
            raise RuntimeError(
                "Build baseline before applying rules."
            )

        # ----------------------------------------------------
        # Previous-hour activity
        # ----------------------------------------------------

        self.data["previous_activity"] = (
            self.data
            .groupby("grid_id")["total_activity"]
            .shift(1)
        )

        # ----------------------------------------------------
        # HIGH_ACTIVITY
        # ----------------------------------------------------

        self.data["high_activity"] = (
            self.data["eligible_for_alert"]
            &
            (
                self.data["total_activity"]
                >=
                self.high_multiplier
                *
                self.data["baseline_activity"]
            )
        )

        # ----------------------------------------------------
        # ACTIVITY_DROP
        # ----------------------------------------------------

        self.data["activity_drop"] = (
            self.data["eligible_for_alert"]
            &
            (
                self.data["total_activity"]
                <=
                self.drop_multiplier
                *
                self.data["baseline_activity"]
            )
        )

        # ----------------------------------------------------
        # ACTIVITY_SPIKE
        # ----------------------------------------------------

        self.data["activity_spike"] = (
            self.data["eligible_for_alert"]
            &
            self.data["previous_activity"].notna()
            &
            (
                self.data["total_activity"]
                >=
                self.spike_multiplier
                *
                self.data["previous_activity"]
            )
        )

        logger.info(
            "Alert rules evaluated."
        )

        return self.data

    # ========================================================
    # 7. Build alert records
    # ========================================================

    def create_alert_records(self):

        alerts = []

        for _, row in self.data.iterrows():

            current = row["total_activity"]
            baseline = row["baseline_activity"]
            previous = row["previous_activity"]

            # ----------------------------------------------
            # HIGH_ACTIVITY
            # ----------------------------------------------

            if row["high_activity"]:

                alerts.append({
                    "grid_id": row["grid_id"],
                    "timestamp": row["timestamp"],
                    "alert_type": "HIGH_ACTIVITY",
                    "current_activity": current,
                    "baseline_activity": baseline,
                    "reason": (
                        f"Current activity {current:.2f} "
                        f"is at least "
                        f"{self.high_multiplier:.2f}x "
                        f"the within-day baseline "
                        f"{baseline:.2f}."
                    )
                })

            # ----------------------------------------------
            # ACTIVITY_SPIKE
            # ----------------------------------------------

            if row["activity_spike"]:

                alerts.append({
                    "grid_id": row["grid_id"],
                    "timestamp": row["timestamp"],
                    "alert_type": "ACTIVITY_SPIKE",
                    "current_activity": current,
                    "baseline_activity": baseline,
                    "reason": (
                        f"Current activity {current:.2f} "
                        f"is at least "
                        f"{self.spike_multiplier:.2f}x "
                        f"the preceding hour "
                        f"{previous:.2f}."
                    )
                })

            # ----------------------------------------------
            # ACTIVITY_DROP
            # ----------------------------------------------

            if row["activity_drop"]:

                alerts.append({
                    "grid_id": row["grid_id"],
                    "timestamp": row["timestamp"],
                    "alert_type": "ACTIVITY_DROP",
                    "current_activity": current,
                    "baseline_activity": baseline,
                    "reason": (
                        f"Current activity {current:.2f} "
                        f"is at most "
                        f"{self.drop_multiplier:.2f}x "
                        f"the within-day baseline "
                        f"{baseline:.2f}."
                    )
                })

        self.alerts = pd.DataFrame(
            alerts,
            columns=[
                "grid_id",
                "timestamp",
                "alert_type",
                "current_activity",
                "baseline_activity",
                "reason"
            ]
        )

        logger.info(
            "Created %d alert records.",
            len(self.alerts)
        )

        return self.alerts

    # ========================================================
    # 8. Compute operational summary
    # ========================================================

    def compute_summary(self):

        if self.alerts is None:
            raise RuntimeError(
                "Create alert records first."
            )

        total_grid_hours = len(
            self.data
        )

        total_alerts = len(
            self.alerts
        )

        # A grid/hour can technically produce multiple
        # alert types.
        alerted_grid_hours = (
            self.alerts[
                ["grid_id", "timestamp"]
            ]
            .drop_duplicates()
            .shape[0]
        )

        proportion_alerted = (
            alerted_grid_hours /
            total_grid_hours
            if total_grid_hours > 0
            else 0
        )

        alerts_by_type = (
            self.alerts["alert_type"]
            .value_counts()
            .to_dict()
        )

        top_grids = (
            self.alerts
            .groupby("grid_id")
            .size()
            .sort_values(ascending=False)
            .head(10)
        )

        self.summary = {
            "total_grid_hours": total_grid_hours,
            "total_alert_records": total_alerts,
            "alerted_grid_hours": alerted_grid_hours,
            "proportion_grid_hours_alerted": (
                proportion_alerted
            ),
            "alerts_by_type": alerts_by_type,
            "top_10_grids": top_grids.to_dict(),
            "activity_floor": self.activity_floor,
            "high_multiplier": self.high_multiplier,
            "spike_multiplier": self.spike_multiplier,
            "drop_multiplier": self.drop_multiplier
        }

        return self.summary

    # ========================================================
    # 9. Export alerts
    # ========================================================

    def export_alerts(
        self,
        output_path="data/curated/activity_alerts.csv"
    ):

        if self.alerts is None:
            raise RuntimeError(
                "Create alert records before exporting."
            )

        output_path = Path(output_path)

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        self.alerts.to_csv(
            output_path,
            index=False
        )

        logger.info(
            "Alerts written to: %s",
            output_path
        )

        return output_path

    # ========================================================
    # 10. Operational summary
    # ========================================================

    def operational_summary(self):

        if self.summary is None:
            self.compute_summary()

        logger.info(
            "========== NP3 OPERATIONAL SUMMARY =========="
        )

        logger.info(
            "Total grid/hours: %d",
            self.summary["total_grid_hours"]
        )

        logger.info(
            "Total alert records: %d",
            self.summary["total_alert_records"]
        )

        logger.info(
            "Grid/hours alerted: %d",
            self.summary["alerted_grid_hours"]
        )

        logger.info(
            "Proportion alerted: %.2f%%",
            self.summary[
                "proportion_grid_hours_alerted"
            ] * 100
        )

        logger.info(
            "Alerts by type: %s",
            self.summary["alerts_by_type"]
        )

        logger.info(
            "Top 10 grids by alert count: %s",
            self.summary["top_10_grids"]
        )

        logger.info(
            "Activity floor: %.4f",
            self.summary["activity_floor"]
        )

        return self.summary