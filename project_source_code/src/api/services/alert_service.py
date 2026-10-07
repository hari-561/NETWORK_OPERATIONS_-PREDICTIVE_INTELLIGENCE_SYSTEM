from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session


class AlertService:

    # ============================================================
    # GET ALERTS
    # ============================================================

    @staticmethod
    def get_alerts(
        db: Session,
        limit: int = 100,
        severity: str | None = None,
        as_of: datetime | None = None,
    ) -> dict:

        # --------------------------------------------------------
        # 1. Determine AS_OF
        # --------------------------------------------------------

        if as_of is None:

            effective_as_of = db.execute(
                text("""
                    SELECT MAX(timestamp)
                    FROM activity_alerts
                """)
            ).scalar()

            if effective_as_of is None:
                raise ValueError(
                    "No alert timestamp available in activity_alerts"
                )

        else:
            effective_as_of = as_of

        # --------------------------------------------------------
        # 2. Validate limit
        # --------------------------------------------------------

        if limit < 1:
            limit = 1

       

        # --------------------------------------------------------
        # 3. Validate severity
        # --------------------------------------------------------

        if severity is not None:

            severity = severity.upper()

            allowed_severities = {
                "HIGH",
                "MEDIUM",
            }

            if severity not in allowed_severities:

                raise ValueError(
                    "Invalid severity. "
                    "Allowed values: HIGH, MEDIUM"
                )

        # --------------------------------------------------------
        # 4. Build query
        # --------------------------------------------------------

        query = """
            SELECT
                grid_id,
                timestamp,
                alert_type,
                severity,
                current_activity,
                baseline_activity,
                reason
            FROM activity_alerts
            WHERE timestamp <= :as_of
        """

        params = {
            "as_of": effective_as_of,
            "limit": limit,
        }

        # --------------------------------------------------------
        # 5. Optional severity filter
        # --------------------------------------------------------

        if severity is not None:

            query += """
                AND severity = :severity
            """

            params["severity"] = severity

        # --------------------------------------------------------
        # 6. Newest alerts first
        # --------------------------------------------------------

        query += """
            ORDER BY timestamp DESC
            LIMIT :limit
        """

        # --------------------------------------------------------
        # 7. Execute query
        # --------------------------------------------------------

        rows = db.execute(
            text(query),
            params,
        ).mappings().all()

        # --------------------------------------------------------
        # 8. Convert database rows to API records
        # --------------------------------------------------------

        data = []

        for row in rows:

            data.append(
                {
                    "grid_id": int(
                        row["grid_id"]
                    ),

                    "timestamp": row["timestamp"],

                    "alert_type": row["alert_type"],

                    "severity": row["severity"],

                    "current_activity": float(
                        row["current_activity"]
                    ),

                    "baseline_activity": (
                        float(row["baseline_activity"])
                        if row["baseline_activity"] is not None
                        else None
                    ),

                    "reason": row["reason"],
                }
            )

        # --------------------------------------------------------
        # 9. Return response
        # --------------------------------------------------------

        return {
            "as_of": effective_as_of,
            "data": data,
        }

    # ============================================================
    # GET HOTSPOTS
    # ============================================================

    @staticmethod
    def get_hotspots(
        db: Session,
        limit: int = 100,
        severity: str | None = None,
        as_of: datetime | None = None,
    ) -> dict:

        # --------------------------------------------------------
        # 1. Determine AS_OF
        # --------------------------------------------------------

        if as_of is None:

            effective_as_of = db.execute(
                text("""
                    SELECT MAX(timestamp)
                    FROM activity_alerts
                """)
            ).scalar()

            if effective_as_of is None:
                raise ValueError(
                    "No alert timestamp available in activity_alerts"
                )

        else:
            effective_as_of = as_of

        # --------------------------------------------------------
        # 2. Validate limit
        # --------------------------------------------------------

        if limit < 1:
            limit = 1

        if limit > 1000:
            limit = 1000

        # --------------------------------------------------------
        # 3. Validate severity
        # --------------------------------------------------------

        if severity is not None:

            severity = severity.upper()

            allowed_severities = {
                "HIGH",
                "MEDIUM",
            }

            if severity not in allowed_severities:

                raise ValueError(
                    "Invalid severity. "
                    "Allowed values: HIGH, MEDIUM"
                )

        # --------------------------------------------------------
        # 4. Build hotspot query
        #
        # A hotspot is a grid that has alert activity.
        #
        # We rank grids by:
        #   1. Number of alerts
        #   2. Most recent alert
        #   3. Highest current activity
        #
        # We then return the latest alert for each hotspot grid.
        # --------------------------------------------------------

        query = """
            SELECT
                a.grid_id,
                a.timestamp,
                a.alert_type,
                a.severity,
                a.current_activity,
                a.baseline_activity,
                a.reason
            FROM activity_alerts a

            INNER JOIN (
                SELECT
                    grid_id,
                    MAX(timestamp) AS latest_timestamp
                FROM activity_alerts
                WHERE timestamp <= :as_of
        """

        params = {
            "as_of": effective_as_of,
            "limit": limit,
        }

        # --------------------------------------------------------
        # 5. Severity filter inside hotspot aggregation
        # --------------------------------------------------------

        if severity is not None:

            query += """
                AND severity = :severity
            """

            params["severity"] = severity

        query += """
                GROUP BY grid_id
            ) latest

                ON a.grid_id = latest.grid_id
                AND a.timestamp = latest.latest_timestamp

            WHERE a.timestamp <= :as_of
        """

        if severity is not None:

            query += """
                AND a.severity = :severity
            """

        # --------------------------------------------------------
        # 6. Rank hotspots
        # --------------------------------------------------------

        query += """
            ORDER BY
                a.current_activity DESC,
                a.timestamp DESC

            LIMIT :limit
        """

        # --------------------------------------------------------
        # 7. Execute
        # --------------------------------------------------------

        rows = db.execute(
            text(query),
            params,
        ).mappings().all()

        # --------------------------------------------------------
        # 8. Convert rows
        # --------------------------------------------------------

        data = []

        for row in rows:

            data.append(
                {
                    "grid_id": int(
                        row["grid_id"]
                    ),

                    "timestamp": row["timestamp"],

                    "alert_type": row["alert_type"],

                    "severity": row["severity"],

                    "current_activity": float(
                        row["current_activity"]
                    ),

                    "baseline_activity": (
                        float(row["baseline_activity"])
                        if row["baseline_activity"] is not None
                        else None
                    ),

                    "reason": row["reason"],
                }
            )

        # --------------------------------------------------------
        # 9. Return
        # --------------------------------------------------------

        return {
            "as_of": effective_as_of,
            "data": data,
        }