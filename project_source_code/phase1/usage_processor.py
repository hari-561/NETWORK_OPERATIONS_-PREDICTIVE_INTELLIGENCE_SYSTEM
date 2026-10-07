import logging
from pathlib import Path

import pandas as pd


# ============================================================
# Logging configuration
# ============================================================

logger = logging.getLogger(__name__)

if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )


# ============================================================
# UsageProcessor
# ============================================================

class UsageProcessor:

    # --------------------------------------------------------
    # Required canonical columns
    # --------------------------------------------------------

    REQUIRED_COLUMNS = [
        "timestamp",
        "grid_id",
        "country_code",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    ACTIVITY_COLUMNS = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    # Curated-layer policy from the project:
    # missing activity values are treated as zero.
    # Missing timestamp/grid_id are not silently converted to zero.
    CURATED_NULL_POLICY = {
        "sms_in": 0,
        "sms_out": 0,
        "call_in": 0,
        "call_out": 0,
        "internet_activity": 0
    }

    def __init__(self, file_path=None, dataframe=None):

        if file_path is None and dataframe is None:
            raise ValueError(
                "Provide either file_path or dataframe."
            )

        if file_path is not None and dataframe is not None:
            raise ValueError(
                "Provide either file_path or dataframe, not both."
            )

        self.file_path = Path(file_path) if file_path else None

        self.dataframe = (
            dataframe.copy()
            if dataframe is not None
            else None
        )

        self.raw_data = None
        self.cleaned_data = None
        self.grid_hour_data = None
        self.daily_summary = None
        self.grid_summary = None
        self.kpis = None

    # ========================================================
    # 1. load_data()
    # ========================================================

    def load_data(self):

        if self.dataframe is not None:

            logger.info(
                "Loading data from DataFrame: %d rows",
                len(self.dataframe)
            )

            self.raw_data = self.dataframe.copy()

        else:

            logger.info(
                "Loading CSV file: %s",
                self.file_path
            )

            if not self.file_path.exists():
                raise FileNotFoundError(
                    f"File not found: {self.file_path}"
                )

            self.raw_data = pd.read_csv(
                self.file_path
            )

            logger.info(
                "Loaded %d rows from %s",
                len(self.raw_data),
                self.file_path
            )

        return self.raw_data

    # ========================================================
    # 2. clean_data()
    # ========================================================

    def clean_data(self):

        if self.raw_data is None:
            raise RuntimeError(
                "Call load_data() before clean_data()."
            )

        df = self.raw_data.copy()
       

        RAW_TO_CANONICAL = {
            "datetime": "timestamp",
            "CellID": "grid_id",
            "countrycode": "country_code",
            "smsin": "sms_in",
            "smsout": "sms_out",
            "callin": "call_in",
            "callout": "call_out",
            "internet": "internet_activity"
       }

        df = df.rename(
            columns=RAW_TO_CANONICAL
        )

        # ----------------------------------------------------
        # Required-column check
        # ----------------------------------------------------

        missing_columns = [
            col
            for col in self.REQUIRED_COLUMNS
            if col not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                "Missing required columns: "
                f"{missing_columns}"
            )

        original_count = len(df)

        # ----------------------------------------------------
        # Standardize timestamp
        # ----------------------------------------------------
       
        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce"
        )
       
        

        # ----------------------------------------------------
        # Standardize grid ID
        # ----------------------------------------------------

        df["grid_id"] = pd.to_numeric(
            df["grid_id"],
            errors="coerce"
        )

        # ----------------------------------------------------
        # Standardize country code
        # ----------------------------------------------------

        df["country_code"] = pd.to_numeric(
            df["country_code"],
            errors="coerce"
        )

        # ----------------------------------------------------
        # Standardize activity fields
        # ----------------------------------------------------

        for col in self.ACTIVITY_COLUMNS:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        # ----------------------------------------------------
        # Check missing timestamp/grid_id
        # ----------------------------------------------------

        missing_timestamp = df["timestamp"].isna().sum()
        missing_grid = df["grid_id"].isna().sum()

        logger.info(
            "Missing timestamp rows: %d",
            missing_timestamp
        )

        logger.info(
            "Missing grid_id rows: %d",
            missing_grid
        )

        # These are structural fields.
        # Do not silently replace them with zero.
        structural_invalid = (
            df["timestamp"].isna()
            | df["grid_id"].isna()
        )

        dropped_structural = structural_invalid.sum()

        if dropped_structural > 0:

            logger.warning(
                "Dropping %d rows with missing timestamp/grid_id",
                dropped_structural
            )

            df = df.loc[
                ~structural_invalid
            ].copy()

        # ----------------------------------------------------
        # Negative activity check
        # ----------------------------------------------------

        negative_mask = (
            df[self.ACTIVITY_COLUMNS] < 0
        ).any(axis=1)

        negative_count = negative_mask.sum()

        if negative_count > 0:

            logger.warning(
                "Found %d rows containing negative activity values",
                negative_count
            )

            raise ValueError(
                f"Negative activity values found in "
                f"{negative_count} rows."
            )

        # ----------------------------------------------------
        # Curated-layer null handling
        # ----------------------------------------------------

        for col, replacement in self.CURATED_NULL_POLICY.items():

            null_count = df[col].isna().sum()

            if null_count > 0:

                logger.info(
                    "Replacing %d null values in %s with %s "
                    "according to curated-layer policy",
                    null_count,
                    col,
                    replacement
                )

                df[col] = df[col].fillna(
                    replacement
                )

        # ----------------------------------------------------
        # Sort data
        # ----------------------------------------------------

        df = df.sort_values(
            ["timestamp", "grid_id", "country_code"]
        ).reset_index(drop=True)

        self.cleaned_data = df

        logger.info(
            "Cleaning complete: input=%d rows, output=%d rows, "
            "dropped=%d rows",
            original_count,
            len(df),
            original_count - len(df)
        )

        return self.cleaned_data

    # ========================================================
    # 3. derive_time_features()
    # ========================================================

    def derive_time_features(self):

        if self.cleaned_data is None:
            raise RuntimeError(
                "Call clean_data() before derive_time_features()."
            )

        df = self.cleaned_data.copy()
        

        df["date"] = (
            df["timestamp"].dt.date
        )

        df["hour"] = (
            df["timestamp"].dt.hour
        )

        df["day_of_week"] = (
            df["timestamp"].dt.day_name()
        )

        self.cleaned_data = df

        logger.info(
            "Derived date, hour and day_of_week features."
        )

        

        return self.cleaned_data

    # ========================================================
    # 4. aggregate_to_grid_time()
    # ========================================================

    def aggregate_to_grid_time(self):

        if self.cleaned_data is None:
            raise RuntimeError(
                "Call derive_time_features() before "
                "aggregate_to_grid_time()."
            )

        df = self.cleaned_data.copy()

        # ----------------------------------------------------
        # Country code intentionally excluded.
        #
        # Multiple country-code rows can exist for the same
        # grid + timestamp.
        #
        # Therefore we aggregate across country codes.
        # ----------------------------------------------------

        group_columns = [
            "timestamp",
            "grid_id"
        ]

        grid_hour = (
            df.groupby(
                group_columns,
                as_index=False
            )[self.ACTIVITY_COLUMNS]
            .sum()
        )

        # Add date/hour/day information

        grid_hour["date"] = (
            grid_hour["timestamp"].dt.date
        )

        grid_hour["hour"] = (
            grid_hour["timestamp"].dt.hour
        )

        grid_hour["day_of_week"] = (
            grid_hour["timestamp"].dt.day_name()
        )

        grid_hour = grid_hour.sort_values(
            ["timestamp", "grid_id"]
        ).reset_index(drop=True)

        self.grid_hour_data = grid_hour

        logger.info(
            "Created grid/hour analytics: %d rows",
            len(grid_hour)
        )

        return self.grid_hour_data

    # ========================================================
    # 5. derive_activity_features()
    # ========================================================

    def derive_activity_features(self):

        if self.grid_hour_data is None:
            raise RuntimeError(
                "Call aggregate_to_grid_time() before "
                "derive_activity_features()."
            )

        df = self.grid_hour_data.copy()

        df["total_sms"] = (
            df["sms_in"]
            + df["sms_out"]
        )

        df["total_calls"] = (
            df["call_in"]
            + df["call_out"]
        )

        df["total_activity"] = (
            df["total_sms"]
            + df["total_calls"]
            + df["internet_activity"]
        )

        self.grid_hour_data = df

        logger.info(
            "Derived total_sms, total_calls and total_activity."
        )

        return self.grid_hour_data

    # ========================================================
    # 6. compute_kpis()
    # ========================================================

    def compute_kpis(self):

        if self.grid_hour_data is None:
            raise RuntimeError(
                "Call derive_activity_features() before "
                "compute_kpis()."
            )

        df = self.grid_hour_data

        # ----------------------------------------------------
        # Daily summary
        # ----------------------------------------------------

        self.daily_summary = (
            df.groupby(
                "date",
                as_index=False
            )
            .agg(
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                internet_activity=(
                    "internet_activity",
                    "sum"
                ),
                total_activity=(
                    "total_activity",
                    "sum"
                ),
                unique_grids=(
                    "grid_id",
                    "nunique"
                )
            )
        )

        # ----------------------------------------------------
        # Grid-level summary
        # ----------------------------------------------------

        self.grid_summary = (
            df.groupby(
                "grid_id",
                as_index=False
            )
            .agg(
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                internet_activity=(
                    "internet_activity",
                    "sum"
                ),
                total_activity=(
                    "total_activity",
                    "sum"
                ),
                active_hours=(
                    "timestamp",
                    "nunique"
                )
            )
        )

        # ----------------------------------------------------
        # Overall KPIs
        # ----------------------------------------------------

        hourly_activity = (
            df.groupby("hour")["total_activity"]
            .sum()
        )

        busiest_hour = (
            hourly_activity.idxmax()
            if not hourly_activity.empty
            else None
        )

        busiest_grid_series = (
            df.groupby("grid_id")["total_activity"]
            .sum()
        )

        busiest_grid = (
            busiest_grid_series.idxmax()
            if not busiest_grid_series.empty
            else None
        )

        self.kpis = {
            "input_rows": len(self.raw_data),
            "cleaned_rows": len(self.cleaned_data),
            "grid_hour_rows": len(df),
            "unique_grids": df["grid_id"].nunique(),
            "time_start": df["timestamp"].min(),
            "time_end": df["timestamp"].max(),
            "busiest_hour": busiest_hour,
            "busiest_grid": busiest_grid,
            "total_sms": df["total_sms"].sum(),
            "total_calls": df["total_calls"].sum(),
            "total_activity": df["total_activity"].sum()
        }

        logger.info(
            "KPI computation complete."
        )

        return self.kpis

    # ========================================================
    # 7. export_summary()
    # ========================================================

    def export_summary(self, output_dir="curated_data"):

        if self.grid_hour_data is None:
            raise RuntimeError(
                "No grid/hour data available for export."
            )

        if self.daily_summary is None:
            raise RuntimeError(
                "Run compute_kpis() before export_summary()."
            )

        output_dir = Path(output_dir)

        output_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        # ----------------------------------------------------
        # Grid/hour analytics
        # ----------------------------------------------------

        grid_hour_path = (
            output_dir /
            "grid_hour_activity.csv"
        )

        self.grid_hour_data["timestamp"] = pd.to_datetime(
            self.grid_hour_data["timestamp"]
        ).dt.strftime("%Y-%m-%d %H:%M:%S")
       

        
        self.grid_hour_data.to_csv(
            grid_hour_path,
            index=False
        )

        # ----------------------------------------------------
        # Daily summary
        # ----------------------------------------------------

        daily_path = (
            output_dir /
            "daily_activity_summary.csv"
        )

        self.daily_summary.to_csv(
            daily_path,
            index=False
        )

        # ----------------------------------------------------
        # Grid summary
        # ----------------------------------------------------

        grid_path = (
            output_dir /
            "grid_activity_summary.csv"
        )

        self.grid_summary.to_csv(
            grid_path,
            index=False
        )

        logger.info(
            "Grid/hour output written to: %s",
            grid_hour_path
        )

        logger.info(
            "Daily summary written to: %s",
            daily_path
        )

        logger.info(
            "Grid summary written to: %s",
            grid_path
        )

        return {
            "grid_hour": grid_hour_path,
            "daily": daily_path,
            "grid": grid_path
        }


cleaner = UsageProcessor("D:/capstone1/data/sms-call-internet-mi-2013-11-01.csv")
