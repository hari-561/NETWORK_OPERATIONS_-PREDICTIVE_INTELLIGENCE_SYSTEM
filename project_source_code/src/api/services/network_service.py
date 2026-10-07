from datetime import datetime,timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session


class NetworkService:

    @staticmethod
    def get_summary(
        db: Session,
        as_of: datetime | None = None,
    ) -> dict:

        # --------------------------------------------------
        # 1. Determine effective AS_OF
        # --------------------------------------------------

        if as_of is None:

            result = db.execute(
                text("""
                    SELECT MAX(timestamp)
                    FROM dim_time
                """)
            ).scalar()

            if result is None:
                raise ValueError(
                    "No timestamp available in dim_time"
                )

            effective_as_of = result

        else:
            effective_as_of = as_of

        # --------------------------------------------------
        # 2. Check that requested timestamp exists
        # --------------------------------------------------

        timestamp_exists = db.execute(
            text("""
                SELECT 1
                FROM dim_time
                WHERE timestamp = :as_of
                LIMIT 1
            """),
            {
                "as_of": effective_as_of
            }
        ).scalar()

        if timestamp_exists is None:
            raise ValueError(
                f"No analytics data available for as_of "
                f"{effective_as_of}"
            )

        # --------------------------------------------------
        # 3. Total activity
        # --------------------------------------------------

        total_activity = db.execute(
            text("""
                SELECT COALESCE(
                    SUM(f.total_activity),
                    0
                )
                FROM fact_network_activity f
                
            """),
            {
                "as_of": effective_as_of
            }
        ).scalar()

        # --------------------------------------------------
        # 4. Active grids
        # --------------------------------------------------

        active_grids = db.execute(
            text("""
                SELECT COUNT(*)
                FROM fact_network_activity f
                JOIN dim_time t
                    ON f.time_key = t.time_key
                WHERE t.timestamp = :as_of
                  AND f.total_activity > 0
            """),
            {
                "as_of": effective_as_of
            }
        ).scalar()

        # --------------------------------------------------
        # 5. Peak hour
        # --------------------------------------------------

        peak_hour = db.execute(
            text("""
                SELECT
                    t.hour_of_day
                FROM fact_network_activity f
                JOIN dim_time t
                    ON f.time_key = t.time_key
                GROUP BY
                    t.hour_of_day
                ORDER BY
                    SUM(f.total_activity) DESC
                LIMIT 1
            """)
        ).scalar()

        if peak_hour is None:
            raise ValueError(
                "Unable to determine peak hour"
            )

        # --------------------------------------------------
        # 6. Top grid for effective AS_OF
        # --------------------------------------------------

        top_grid = db.execute(
            text("""
                SELECT
                    g.grid_id,sum(f.total_activity) as activity
                FROM fact_network_activity f
                JOIN dim_time t
                    ON f.time_key = t.time_key
                JOIN dim_grid g
                    ON f.grid_key = g.grid_key
                WHERE t.timestamp <= :as_of
                GROUP BY g.grid_id
                ORDER BY
                    activity DESC
                LIMIT 1
            """),

            {
                "as_of": effective_as_of
            }
        ).scalar()

        if top_grid is None:
            raise ValueError(
                f"No network activity available for "
                f"{effective_as_of}"
            )

        # --------------------------------------------------
        # 7. Return API response
        # --------------------------------------------------

        return {
            "total_activity": float(total_activity or 0),
            "active_grids": int(active_grids or 0),
            "peak_hour": int(peak_hour),
            "top_grid": int(top_grid),
            "as_of": effective_as_of,
        }

    @staticmethod
    def get_grid_activity(
        db: Session,
        grid_id: int,
        date: str | None = None,
        hour: int | None = None,
        as_of: datetime | None = None,
    ) -> dict:

        # --------------------------------------------------
        # 1. Validate grid range
        # --------------------------------------------------

        if grid_id < 1 or grid_id > 10000:
            raise LookupError(
                f"Grid {grid_id} not found"
            )

        # --------------------------------------------------
        # 2. Verify grid exists in dim_grid
        # --------------------------------------------------

        grid_exists = db.execute(
            text("""
                SELECT 1
                FROM dim_grid
                WHERE grid_id = :grid_id
                LIMIT 1
            """),
            {
                "grid_id": grid_id
            }
        ).scalar()

        if grid_exists is None:
            raise LookupError(
                f"Grid {grid_id} not found"
            )

        # --------------------------------------------------
        # 3. Determine AS_OF
        # --------------------------------------------------

        if as_of is None:

            effective_as_of = db.execute(
                text("""
                    SELECT MAX(timestamp)
                    FROM dim_time
                """)
            ).scalar()

            if effective_as_of is None:
                raise ValueError(
                    "No timestamp available in dim_time"
                )

        else:
            effective_as_of = as_of

        # --------------------------------------------------
        # 4. Validate AS_OF exists
        # --------------------------------------------------

        as_of_exists = db.execute(
            text("""
                SELECT 1
                FROM dim_time
                WHERE timestamp = :as_of
                LIMIT 1
            """),
            {
                "as_of": effective_as_of
            }
        ).scalar()

        if as_of_exists is None:
            raise ValueError(
                f"No analytics data available for "
                f"as_of {effective_as_of}"
            )

        # --------------------------------------------------
        # 5. Determine requested window
        # --------------------------------------------------

        # --------------------------------------------------
# 5. Determine requested window
# --------------------------------------------------

        if date is None and hour is None:

            # Default:
            # trailing 24 hourly intervals ending at AS_OF

            start_time = (
                effective_as_of - timedelta(hours=23)
            )

            end_time = effective_as_of

            params = {
                "grid_id": grid_id,
                "start_time": start_time,
                "end_time": end_time,
            }

            rows = db.execute(
                text("""
                    SELECT
                        t.timestamp,
                        t.hour_of_day,
                        f.total_sms,
                        f.total_calls,
                        f.internet_activity,
                        f.total_activity
                    FROM fact_network_activity f
                    JOIN dim_grid g
                        ON f.grid_key = g.grid_key
                    JOIN dim_time t
                        ON f.time_key = t.time_key
                    WHERE g.grid_id = :grid_id
                    AND t.timestamp BETWEEN
                        :start_time AND :end_time
                    ORDER BY t.timestamp
                """),
                params,
            ).mappings().all()

        else:

            # --------------------------------------------------
            # Filtered queries
            # --------------------------------------------------

            conditions = [
                "g.grid_id = :grid_id"
            ]

            params = {
                "grid_id": grid_id
            }

            # ----------------------------------------------
            # DATE + HOUR
            #
            # Meaning:
            # selected hour from selected date
            # through effective AS_OF
            # ----------------------------------------------

            if date is not None and hour is not None:

                conditions.append(
                    "t.date_value >= :start_date"
                )

                conditions.append(
                    "t.timestamp <= :as_of"
                )

                conditions.append(
                    "t.hour_of_day = :hour"
                )

                params["start_date"] = date
                params["as_of"] = effective_as_of
                params["hour"] = hour

            # ----------------------------------------------
            # DATE ONLY
            #
            # Meaning:
            # all hours on that date
            # ----------------------------------------------

            elif date is not None:

                conditions.append(
                    "t.date_value = :date"
                )

                conditions.append(
                    "t.timestamp <= :as_of"
                )

                params["date"] = date
                params["as_of"] = effective_as_of

            # ----------------------------------------------
            # HOUR ONLY
            #
            # Meaning:
            # selected hour across all available dates
            # up to AS_OF
            # ----------------------------------------------

            elif hour is not None:

                conditions.append(
                    "t.hour_of_day = :hour"
                )

                conditions.append(
                    "t.timestamp <= :as_of"
                )

                params["hour"] = hour
                params["as_of"] = effective_as_of

            where_clause = " AND ".join(conditions)

            rows = db.execute(
                text(f"""
                    SELECT
                        t.timestamp,
                        t.hour_of_day,
                        f.total_sms,
                        f.total_calls,
                        f.internet_activity,
                        f.total_activity
                    FROM fact_network_activity f
                    JOIN dim_grid g
                        ON f.grid_key = g.grid_key
                    JOIN dim_time t
                        ON f.time_key = t.time_key
                    WHERE {where_clause}
                    ORDER BY t.timestamp
                """),
                params,
            ).mappings().all()

            if rows:
                start_time = rows[0]["timestamp"]
                end_time = rows[-1]["timestamp"]

            else:
                start_time = effective_as_of
                end_time = effective_as_of

        # --------------------------------------------------
        # 6. No activity rows
        # --------------------------------------------------

        if not rows:
            raise ValueError(
                f"No activity data available for grid "
                f"{grid_id} in the requested window"
            )

        # --------------------------------------------------
        # 7. Build response
        # --------------------------------------------------

        data = [
            {
                "timestamp": row["timestamp"],
                "hour_of_day": int(row["hour_of_day"]),
                "total_sms": float(row["total_sms"]),
                "total_calls": float(row["total_calls"]),
                "internet_activity": float(
                    row["internet_activity"]
                ),
                "total_activity": float(
                    row["total_activity"]
                ),
            }
            for row in rows
        ]

        return {
            "grid_id": grid_id,
            "as_of": effective_as_of,
            "start_time": start_time,
            "end_time": end_time,
            "data": data,
        }





    @staticmethod
    def get_grid_features(
        db: Session,
        grid_id: int,
        as_of: datetime | None = None,
    ) -> dict:

        # --------------------------------------------------
        # 1. Validate grid range
        # --------------------------------------------------

        if grid_id < 1 or grid_id > 10000:
            raise LookupError(
                f"Grid {grid_id} not found"
            )

        # --------------------------------------------------
        # 2. Read stored ML2 feature row
        #
        # If as_of is provided, get the latest feature
        # available at or before that timestamp.
        #
        # If as_of is not provided, get the latest row.
        # --------------------------------------------------

        if as_of is not None:

            row = db.execute(
                text("""
                    SELECT
                        grid_id,
                        feature_timestamp,
                        avg_activity,
                        activity_growth,
                        active_hours,
                        peak_ratio,
                        variability,
                        internet_share
                    FROM network_feature_table
                    WHERE grid_id = :grid_id
                    AND feature_timestamp <= :as_of
                    ORDER BY feature_timestamp DESC
                    LIMIT 1
                """),
                {
                    "grid_id": grid_id,
                    "as_of": as_of,
                },
            ).mappings().first()

        else:

            row = db.execute(
                text("""
                    SELECT
                        grid_id,
                        feature_timestamp,
                        avg_activity,
                        activity_growth,
                        active_hours,
                        peak_ratio,
                        variability,
                        internet_share
                    FROM network_feature_table
                    WHERE grid_id = :grid_id
                    ORDER BY feature_timestamp DESC
                    LIMIT 1
                """),
                {
                    "grid_id": grid_id,
                },
            ).mappings().first()

        # --------------------------------------------------
        # 3. No stored feature row
        # --------------------------------------------------

        if row is None:
            raise LookupError(
                f"No stored features found for grid {grid_id}"
                + (
                    f" at or before {as_of}"
                    if as_of is not None
                    else ""
                )
            )

        # --------------------------------------------------
        # 4. Data-quality validation
        # --------------------------------------------------

        feature_names = [
            "avg_activity",
            "activity_growth",
            "active_hours",
            "peak_ratio",
            "variability",
            "internet_share",
        ]

        missing_features = [
            name
            for name in feature_names
            if row[name] is None
        ]

        if missing_features:
            raise ValueError(
                "Stored feature row is incomplete. "
                f"Missing: {', '.join(missing_features)}"
            )

        # --------------------------------------------------
        # 5. Check for invalid numeric values
        # --------------------------------------------------

        import math

        numeric_values = [
            row["avg_activity"],
            row["activity_growth"],
            row["peak_ratio"],
            row["variability"],
            row["internet_share"],
        ]

        if any(
            not math.isfinite(float(value))
            for value in numeric_values
        ):
            raise ValueError(
                "Stored feature row contains "
                "non-finite values"
            )

        # --------------------------------------------------
        # 6. Determine data quality
        # --------------------------------------------------

        data_quality = "GOOD"

        # --------------------------------------------------
        # 7. Determine feature freshness
        # --------------------------------------------------

        latest_timestamp = db.execute(
            text("""
                SELECT MAX(timestamp)
                FROM dim_time
            """)
        ).scalar()

        if latest_timestamp is None:
            raise ValueError(
                "Unable to determine analytics freshness"
            )

        feature_timestamp = row["feature_timestamp"]

        if feature_timestamp >= latest_timestamp:

            feature_freshness = "CURRENT"

        else:

            age_hours = (
                latest_timestamp
                - feature_timestamp
            ).total_seconds() / 3600

            if age_hours <= 24:
                feature_freshness = "RECENT"
            else:
                feature_freshness = "STALE"

        # --------------------------------------------------
        # 8. Return stored values
        # --------------------------------------------------

        return {
            "grid_id": int(row["grid_id"]),

            "avg_activity": float(
                row["avg_activity"]
            ),

            "activity_growth": float(
                row["activity_growth"]
            ),

            "active_hours": int(
                row["active_hours"]
            ),

            "peak_ratio": float(
                row["peak_ratio"]
            ),

            "variability": float(
                row["variability"]
            ),

            "internet_share": float(
                row["internet_share"]
            ),

            "feature_timestamp": feature_timestamp,

            "data_quality": data_quality,
            "feature_freshness": feature_freshness,
        }




    
    @staticmethod
    def get_grid_features2(
        db: Session,
        grid_id: int,
        as_of: datetime | None = None,
    ) -> dict:

        # --------------------------------------------------
        # 1. Validate grid range
        # --------------------------------------------------

        if grid_id < 1 or grid_id > 10000:
            raise LookupError(
                f"Grid {grid_id} not found"
            )

        # --------------------------------------------------
        # 2. Read stored ML2 feature row
        #
        # If as_of is provided, get the latest feature
        # available at or before that timestamp.
        #
        # If as_of is not provided, get the latest row.
        # --------------------------------------------------

        if as_of is not None:

            row = db.execute(
                text("""
                    SELECT
                        grid_id,
                        feature_timestamp,
                        avg_activity,
                        activity_growth,
                        active_hours,
                        peak_ratio,
                        variability,
                        internet_share,
                        trailing_median_24h,
                        hour_of_day,
                        day_of_week
                    FROM feature_table2
                    WHERE grid_id = :grid_id
                    AND feature_timestamp <= :as_of
                    ORDER BY feature_timestamp DESC
                    LIMIT 1
                """),
                {
                    "grid_id": grid_id,
                    "as_of": as_of,
                },
            ).mappings().first()

        else:

            row = db.execute(
                text("""
                    SELECT
                        grid_id,
                        feature_timestamp,
                        avg_activity,
                        activity_growth,
                        active_hours,
                        peak_ratio,
                        variability,
                        internet_share,
                        trailing_median_24h,
                        hour_of_day,
                        day_of_week
                    FROM feature_table2
                    WHERE grid_id = :grid_id
                    ORDER BY feature_timestamp DESC
                    LIMIT 1
                """),
                {
                    "grid_id": grid_id,
                },
            ).mappings().first()

        # --------------------------------------------------
        # 3. No stored feature row
        # --------------------------------------------------

        if row is None:
            raise LookupError(
                f"No stored features found for grid {grid_id}"
                + (
                    f" at or before {as_of}"
                    if as_of is not None
                    else ""
                )
            )

        # --------------------------------------------------
        # 4. Data-quality validation
        # --------------------------------------------------

        feature_names = [
            "avg_activity",
            "activity_growth",
            "active_hours",
            "peak_ratio",
            "variability",
            "internet_share",
            "trailing_median_24h",
            "hour_of_day",
            "day_of_week"
        ]

        missing_features = [
            name
            for name in feature_names
            if row[name] is None
        ]

        if missing_features:
            raise ValueError(
                "Stored feature row is incomplete. "
                f"Missing: {', '.join(missing_features)}"
            )

        # --------------------------------------------------
        # 5. Check for invalid numeric values
        # --------------------------------------------------

        import math

        numeric_values = [
            row["avg_activity"],
            row["activity_growth"],
            row["peak_ratio"],
            row["variability"],
            row["internet_share"],
            row["trailing_median_24h"],
            row["hour_of_day"],
            row["day_of_week"]
        ]

        if any(
            not math.isfinite(float(value))
            for value in numeric_values
        ):
            raise ValueError(
                "Stored feature row contains "
                "non-finite values"
            )

        # --------------------------------------------------
        # 6. Determine data quality
        # --------------------------------------------------

        data_quality = "GOOD"

        # --------------------------------------------------
        # 7. Determine feature freshness
        # --------------------------------------------------

        latest_timestamp = db.execute(
            text("""
                SELECT MAX(timestamp)
                FROM dim_time
            """)
        ).scalar()

        if latest_timestamp is None:
            raise ValueError(
                "Unable to determine analytics freshness"
            )

        feature_timestamp = row["feature_timestamp"]

        if feature_timestamp >= latest_timestamp:

            feature_freshness = "CURRENT"

        else:

            age_hours = (
                latest_timestamp
                - feature_timestamp
            ).total_seconds() / 3600

            if age_hours <= 24:
                feature_freshness = "RECENT"
            else:
                feature_freshness = "STALE"

        # --------------------------------------------------
        # 8. Return stored values
        # --------------------------------------------------

        return {
            "grid_id": int(row["grid_id"]),

            "avg_activity": float(
                row["avg_activity"]
            ),

            "activity_growth": float(
                row["activity_growth"]
            ),

            "active_hours": int(
                row["active_hours"]
            ),

            "peak_ratio": float(
                row["peak_ratio"]
            ),

            "variability": float(
                row["variability"]
            ),

            "internet_share": float(
                row["internet_share"]
            ),

            "feature_timestamp": feature_timestamp,

            "trailing_median_24h": float(
                row["trailing_median_24h"]),
            "hour_of_day": int(
                row["hour_of_day"]),
            "day_of_week": int(
                row["day_of_week"]),    
            "data_quality": data_quality,

            "feature_freshness": feature_freshness,
        }

    @staticmethod
    def get_grid_location(
        db: Session,
        grid_id: int,
    ) -> dict:

        row = db.execute(
            text("""
                SELECT
                    grid_id,
                    centroid_latitude,
                    centroid_longitude,
                    geometry_ref
                FROM dim_grid
                WHERE grid_id = :grid_id
                LIMIT 1
            """),
            {
                "grid_id": grid_id,
            },
        ).mappings().first()

        if row is None:
            raise LookupError(
                f"Grid {grid_id} not found"
            )

        return {
            "grid_id": int(row["grid_id"]),
            "centroid_latitude": (
                float(row["centroid_latitude"])
                if row["centroid_latitude"] is not None
                else None
            ),
            "centroid_longitude": (
                float(row["centroid_longitude"])
                if row["centroid_longitude"] is not None
                else None
            ),
            "geometry_ref": row["geometry_ref"],
        }

    @staticmethod
    def get_anomaly_score(
        db: Session,
        grid_id: int,
        as_of: datetime | None = None,
    ) -> dict:

        # --------------------------------------------------
        # 1. Validate grid range
        # --------------------------------------------------

        if grid_id < 1 or grid_id > 10000:
            raise LookupError(
                f"Grid {grid_id} not found"
            )

        # --------------------------------------------------
        # 2. Get latest anomaly record
        #    at or before AS_OF
        # --------------------------------------------------

        if as_of is not None:

            row = db.execute(
                text("""
                    SELECT
                        grid_id,
                        timestamp,
                        hour_of_day,
                        total_activity,
                        historical_baseline,
                        baseline_count,
                        deviation,
                        anomaly_score,
                        anomaly_direction,
                        anomaly_flag,
                        reason,
                        recommended_action
                    FROM anomaly_detection_data
                    WHERE grid_id = :grid_id
                      AND timestamp <= :as_of
                    ORDER BY timestamp DESC
                    LIMIT 1
                """),
                {
                    "grid_id": grid_id,
                    "as_of": as_of,
                },
            ).mappings().first()

        else:

            row = db.execute(
                text("""
                    SELECT
                        grid_id,
                        timestamp,
                        hour_of_day,
                        total_activity,
                        historical_baseline,
                        baseline_count,
                        deviation,
                        anomaly_score,
                        anomaly_direction,
                        anomaly_flag,
                        reason,
                        recommended_action
                    FROM anomaly_detection_data
                    WHERE grid_id = :grid_id
                    ORDER BY timestamp DESC
                    LIMIT 1
                """),
                {
                    "grid_id": grid_id,
                },
            ).mappings().first()

        # --------------------------------------------------
        # 3. No anomaly record
        # --------------------------------------------------

        if row is None:
            raise LookupError(
                f"No anomaly data found for grid {grid_id}"
                + (
                    f" at or before {as_of}"
                    if as_of is not None
                    else ""
                )
            )

        # --------------------------------------------------
        # 4. Return stored anomaly evidence
        # --------------------------------------------------

        return {
            "grid_id": int(row["grid_id"]),
            "timestamp": row["timestamp"],
            "hour_of_day": int(row["hour_of_day"]),
            "total_activity": float(row["total_activity"]),
            "historical_baseline": (
                float(row["historical_baseline"])
                if row["historical_baseline"] is not None
                else None
            ),
            "baseline_count": (
                int(row["baseline_count"])
                if row["baseline_count"] is not None
                else None
            ),
            "deviation": (
                float(row["deviation"])
                if row["deviation"] is not None
                else None
            ),
            "anomaly_score": (
                float(row["anomaly_score"])
                if row["anomaly_score"] is not None
                else None
            ),
            "anomaly_direction": row["anomaly_direction"],
            "anomaly_flag": bool(row["anomaly_flag"]),
            "reason": row["reason"],
            "recommended_action": row["recommended_action"],
        }