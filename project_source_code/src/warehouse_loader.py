import argparse
import json
import logging
import math
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession, functions as F


# ============================================================
# WINDOWS / HADOOP CONFIG
# ============================================================

os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["PATH"] = (
    r"D:\hadoop\bin"
    + os.pathsep
    + os.environ.get("PATH", "")
)

# ============================================================
# CONFIGURATION
# ============================================================

MYSQL_JDBC_JAR = (
    r"/mnt/d/mysql-connector-j-26.7.0/mysql-connector-j-26.7.0.jar"
)
# MYSQL_JDBC_JAR = (
#     r"D:/mysql-connector-j-26.7.0/mysql-connector-j-26.7.0.jar"
# )

MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

# Keep the connection configuration here.
# Only the two input file paths are supplied from the command line.
# JDBC_URL = "jdbc:mysql://localhost:3306/telecom_analytics"
JDBC_URL = "jdbc:mysql://172.19.144.1:3306/telecom_analytics"
JDBC_PROPERTIES = {
    "user": "root",
    "password": "root",
    "driver": MYSQL_DRIVER,
}

# MYSQL_URL = "mysql+pymysql://root:root@localhost:3306/telecom_analytics"
MYSQL_URL = "mysql+pymysql://root:root@172.19.144.1:3306/telecom_analytics"

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("warehouse_loader")


# ============================================================
# DATABASE DDL
# ============================================================

CREATE_DATABASE_SQL = """
CREATE DATABASE IF NOT EXISTS telecom_analytics
"""

CREATE_DIM_TIME_SQL = """
CREATE TABLE IF NOT EXISTS dim_time (
    time_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME NOT NULL,
    date_value DATE NOT NULL,
    hour_of_day TINYINT NOT NULL,
    day_of_week TINYINT NOT NULL,
    UNIQUE KEY uq_dim_time_timestamp (timestamp),
    INDEX idx_dim_time_date (date_value),
    INDEX idx_dim_time_hour (hour_of_day)
)
"""

CREATE_DIM_GRID_SQL = """
CREATE TABLE IF NOT EXISTS dim_grid (
    grid_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    grid_id INT NOT NULL,
    centroid_latitude DOUBLE NULL,
    centroid_longitude DOUBLE NULL,
    geometry_ref VARCHAR(500) NULL,
    UNIQUE KEY uq_dim_grid_grid_id (grid_id),
    INDEX idx_dim_grid_grid_id (grid_id)
)
"""

CREATE_FACT_SQL = """
CREATE TABLE IF NOT EXISTS fact_network_activity (
    activity_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    grid_key BIGINT NOT NULL,
    time_key BIGINT NOT NULL,
    sms_in DOUBLE NOT NULL DEFAULT 0,
    sms_out DOUBLE NOT NULL DEFAULT 0,
    call_in DOUBLE NOT NULL DEFAULT 0,
    call_out DOUBLE NOT NULL DEFAULT 0,
    internet_activity DOUBLE NOT NULL DEFAULT 0,
    total_sms DOUBLE NOT NULL DEFAULT 0,
    total_calls DOUBLE NOT NULL DEFAULT 0,
    total_activity DOUBLE NOT NULL DEFAULT 0,

    CONSTRAINT fk_fact_grid
        FOREIGN KEY (grid_key)
        REFERENCES dim_grid(grid_key),

    CONSTRAINT fk_fact_time
        FOREIGN KEY (time_key)
        REFERENCES dim_time(time_key),

    UNIQUE KEY uq_fact_grid_time (
        grid_key,
        time_key
    ),

    INDEX idx_fact_grid (
        grid_key
    ),

    INDEX idx_fact_time (
        time_key
    ),

    INDEX idx_fact_grid_time (
        grid_key,
        time_key
    )
)
"""


# ============================================================
# WAREHOUSE LOADER
# ============================================================

class WarehouseLoader:

    def __init__(
        self,
        hourly_path,
        geojson_path,
    ):
        self.hourly_path = Path(hourly_path)
        self.geojson_path = Path(geojson_path)

        # One SQLAlchemy engine for the complete run.
        # The engine manages a connection pool; individual DB
        # connections are acquired/released with context managers.
        self.engine = None

        # One SparkSession for the complete run.
        self.spark = None

    # ========================================================
    # RESOURCE INITIALIZATION
    # ========================================================

    def connect(self):
        logger.info("Connecting to MySQL warehouse")

        self.engine = create_engine(
            MYSQL_URL,
            pool_pre_ping=True,
        )

        with self.engine.begin() as conn:
            conn.execute(text(CREATE_DATABASE_SQL))

        logger.info("MySQL connection established")

        logger.info("Starting Spark session")

        self.spark = (
            SparkSession.builder
            .appName("TelecomWarehouseLoader")
            .master("local[4]")
            # .config("spark.driver.extraClassPath", MYSQL_JDBC_JAR)
            # .config("spark.executor.extraClassPath", MYSQL_JDBC_JAR)
            .config("spark.jars", MYSQL_JDBC_JAR)
            .getOrCreate()
        )

        logger.info("Spark session established")

    # ========================================================
    # RESOURCE CLEANUP
    # ========================================================

    def close(self):
        logger.info("Closing warehouse resources")

        if self.spark is not None:
            try:
                self.spark.stop()
                logger.info("Spark session stopped")
            finally:
                self.spark = None

        if self.engine is not None:
            try:
                self.engine.dispose()
                logger.info("MySQL engine disposed")
            finally:
                self.engine = None

        logger.info("All warehouse resources closed")

    # ========================================================
    # CREATE TABLES
    # ========================================================

    def create_tables(self):

        logger.info(
            "Creating warehouse tables"
        )

        with self.engine.begin() as conn:

            conn.execute(
                text(CREATE_DIM_TIME_SQL)
            )

            conn.execute(
                text(CREATE_DIM_GRID_SQL)
            )

            conn.execute(
                text(CREATE_FACT_SQL)
            )

        logger.info(
            "Warehouse tables ready"
        )

    # ========================================================
    # LOAD STATIC GRID DIMENSION
    # ========================================================

    def load_grid_dimension(self):

        logger.info(
            "Loading static Milan grid reference"
        )

        if not self.geojson_path.exists():

            raise FileNotFoundError(
                f"GeoJSON not found: "
                f"{self.geojson_path}"
            )

        with open(
            self.geojson_path,
            "r",
            encoding="utf-8",
        ) as file:

            geojson = json.load(file)

        rows = []

        for feature in geojson.get(
            "features",
            []
        ):

            properties = feature.get(
                "properties",
                {}
            )

            # ------------------------------------------------
            # IMPORTANT:
            # ALWAYS use properties.cellId.
            #
            # Do NOT use feature["id"].
            #
            # feature["id"] is 0-based.
            # properties["cellId"] is 1-based.
            # ------------------------------------------------

            grid_id = properties.get(
                "cellId"
            )

            if grid_id is None:
                continue

            geometry = feature.get(
                "geometry",
                {}
            )

            geometry_type = geometry.get(
                "type"
            )

            coordinates = geometry.get(
                "coordinates"
            )

            centroid_latitude = None
            centroid_longitude = None

            # ------------------------------------------------
            # GeoJSON uses:
            #
            # [longitude, latitude]
            # ------------------------------------------------

            if (
                geometry_type == "Polygon"
                and coordinates
                and coordinates[0]
            ):

                outer_ring = coordinates[0]

                longitudes = [
                    point[0]
                    for point in outer_ring
                ]

                latitudes = [
                    point[1]
                    for point in outer_ring
                ]

                centroid_longitude = (
                    sum(longitudes)
                    / len(longitudes)
                )

                centroid_latitude = (
                    sum(latitudes)
                    / len(latitudes)
                )

            rows.append(
                {
                    "grid_id": int(grid_id),

                    "centroid_latitude":
                        centroid_latitude,

                    "centroid_longitude":
                        centroid_longitude,

                    "geometry_ref":
                        f"{self.geojson_path.name}"
                        f"#{grid_id}",
                }
            )

        if not rows:

            raise ValueError(
                "No valid grid features found"
            )

        grid_df = pd.DataFrame(rows)

        grid_df = grid_df.drop_duplicates(
            subset=["grid_id"]
        )

        logger.info(
            "Grid dimension records: %d",
            len(grid_df)
        )

        # ----------------------------------------------------
        # DE6 validation
        # ----------------------------------------------------

        assert (
            grid_df["grid_id"].min()
            >= 1
        )

        assert (
            grid_df["grid_id"].max()
            <= 10000
        )

        assert (
            grid_df["grid_id"].nunique()
            == len(grid_df)
        )

        # ----------------------------------------------------
        # Insert/update dimension
        # ----------------------------------------------------

        with self.engine.begin() as conn:

            for _, row in grid_df.iterrows():

                latitude = (
                    row[
                        "centroid_latitude"
                    ]
                )

                longitude = (
                    row[
                        "centroid_longitude"
                    ]
                )

                # Convert NaN -> None
                if (
                    latitude is None
                    or pd.isna(latitude)
                    or not math.isfinite(
                        float(latitude)
                    )
                ):
                    latitude = None
                else:
                    latitude = float(
                        latitude
                    )

                if (
                    longitude is None
                    or pd.isna(longitude)
                    or not math.isfinite(
                        float(longitude)
                    )
                ):
                    longitude = None
                else:
                    longitude = float(
                        longitude
                    )

                conn.execute(
                    text(
                        """
                        INSERT INTO dim_grid (
                            grid_id,
                            centroid_latitude,
                            centroid_longitude,
                            geometry_ref
                        )
                        VALUES (
                            :grid_id,
                            :latitude,
                            :longitude,
                            :geometry_ref
                        )
                        ON DUPLICATE KEY UPDATE

                            centroid_latitude =
                                VALUES(
                                    centroid_latitude
                                ),

                            centroid_longitude =
                                VALUES(
                                    centroid_longitude
                                ),

                            geometry_ref =
                                VALUES(
                                    geometry_ref
                                )
                        """
                    ),
                    {
                        "grid_id": int(
                            row["grid_id"]
                        ),

                        "latitude": latitude,

                        "longitude": longitude,

                        "geometry_ref":
                            row["geometry_ref"],
                    },
                )

        logger.info(
            "dim_grid populated successfully"
        )



    # ========================================================
    # LOAD TIME DIMENSION
    # ========================================================

    def load_time_dimension(self):

        logger.info(
            "Loading time dimension from Spark output"
        )

        # Reuse the universal SparkSession.
        spark = self.spark

        try:

            # ----------------------------------------------------
            # Read EXACTLY the same Spark output used by fact
            # ----------------------------------------------------

            hourly = (
                spark.read
                .parquet(
                    str(self.hourly_path)
                )
            )

            required_columns = [
                "timestamp",
                "grid_id",
            ]

            missing = [
                column
                for column in required_columns
                if column not in hourly.columns
            ]

            if missing:

                raise ValueError(
                    "Missing required columns for dim_time: "
                    f"{missing}"
                )

            # ----------------------------------------------------
            # Normalize timestamp exactly once
            # ----------------------------------------------------

            hourly = (
                hourly
                .withColumn(
                    "timestamp",
                    F.date_trunc(
                        "hour",
                        F.col("timestamp")
                    )
                )
            )

            # ----------------------------------------------------
            # Create DISTINCT time records
            # ----------------------------------------------------

            time_df = (
                hourly
                .select("timestamp")
                .distinct()
                .withColumn(
                    "date_value",
                    F.to_date(
                        F.col("timestamp")
                    )
                )
                .withColumn(
                    "hour_of_day",
                    F.hour(
                        F.col("timestamp")
                    )
                )
                .withColumn(
                    "day_of_week",
                    F.dayofweek(
                        F.col("timestamp")
                    ) - 1
                )
                .orderBy("timestamp")
            )

            time_count = time_df.count()

            logger.info(
                "Distinct timestamps found: %d",
                time_count
            )

            if time_count == 0:

                raise ValueError(
                    "No timestamps found in "
                    "hourly_grid_summary"
                )

            # ----------------------------------------------------
            # Convert small dimension to Pandas
            #
            # This is safe because dim_time has only one row
            # per timestamp, not one row per activity record.
            # ----------------------------------------------------

            pdf = time_df.toPandas()

            # ----------------------------------------------------
            # Insert into MySQL
            # ----------------------------------------------------

            with self.engine.begin() as conn:

                for _, row in pdf.iterrows():

                    timestamp = row["timestamp"]

                    if pd.isna(timestamp):

                        raise ValueError(
                            "NULL timestamp found "
                            "while loading dim_time"
                        )

                    conn.execute(
                        text(
                            """
                            INSERT INTO dim_time (
                                timestamp,
                                date_value,
                                hour_of_day,
                                day_of_week
                            )
                            VALUES (
                                :timestamp,
                                :date_value,
                                :hour_of_day,
                                :day_of_week
                            )
                            ON DUPLICATE KEY UPDATE

                                date_value =
                                    VALUES(date_value),

                                hour_of_day =
                                    VALUES(hour_of_day),

                                day_of_week =
                                    VALUES(day_of_week)
                            """
                        ),
                        {
                            "timestamp":
                                timestamp.to_pydatetime(),

                            "date_value":
                                row["date_value"],

                            "hour_of_day":
                                int(
                                    row["hour_of_day"]
                                ),

                            "day_of_week":
                                int(
                                    row["day_of_week"]
                                ),
                        },
                    )

            logger.info(
                "dim_time populated successfully"
            )

        finally:
            # DO NOT stop Spark here.
            # The same session is reused by load_fact_with_spark().
            pass

    # ========================================================
    # LOAD FACT USING SPARK
    # ========================================================

    def load_fact_with_spark(self):

        logger.info(
            "Starting Spark fact loading"
        )

        # Reuse the universal SparkSession.
        spark = self.spark

        try:

            # =================================================
            # READ HOURLY ANALYTICS
            # =================================================

            hourly = (
                spark.read
                .parquet(
                    str(self.hourly_path)
                )
            )

            logger.info(
                "Hourly output schema:"
            )

            hourly.printSchema()

            hourly_count = hourly.count()

            logger.info(
                "hourly_grid_summary rows: %d",
                hourly_count
            )

            if hourly_count == 0:

                raise ValueError(
                    "hourly_grid_summary is empty. "
                    "Fact table cannot be populated."
                )

            # =================================================
            # CHECK ACTUAL COLUMN NAMES
            # =================================================

            required_columns = [
                "grid_id",
                "timestamp",
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet_activity",
                "total_sms",
                "total_calls",
                "total_activity",
            ]

            missing_columns = [
                column
                for column in required_columns
                if column not in hourly.columns
            ]

            if missing_columns:

                raise ValueError(
                    "hourly_grid_summary has "
                    "unexpected/missing columns: "
                    f"{missing_columns}"
                )

            # =================================================
            # NORMALIZE JOIN KEYS
            # =================================================

            hourly = (
                hourly
                .withColumn(
                    "grid_id",
                    F.col(
                        "grid_id"
                    ).cast("int")
                )
                .withColumn(
                    "timestamp",
                    F.date_trunc(
                        "hour",
                        F.col(
                            "timestamp"
                        )
                    )
                )
            )

            # =================================================
            # READ DIM_GRID
            # =================================================

            dim_grid = (
                spark.read
                .format("jdbc")
                .option(
                    "url",
                    JDBC_URL
                )
                .options(
                    **JDBC_PROPERTIES
                )
                .option(
                    "dbtable",
                    "dim_grid"
                )
                .load()
                .select(
                    F.col(
                        "grid_key"
                    ).cast("long"),

                    F.col(
                        "grid_id"
                    ).cast("int"),
                )
            )

            grid_count = dim_grid.count()

            logger.info(
                "dim_grid rows read by Spark: %d",
                grid_count
            )

            # =================================================
            # READ DIM_TIME
            # =================================================

            dim_time = (
                spark.read
                .format("jdbc")
                .option(
                    "url",
                    JDBC_URL
                )
                .options(
                    **JDBC_PROPERTIES
                )
                .option(
                    "dbtable",
                    "dim_time"
                )
                .load()
                .select(
                    F.col(
                        "time_key"
                    ).cast("long"),

                    F.date_trunc(
                        "hour",
                        F.col(
                            "timestamp"
                        )
                    ).alias(
                        "timestamp"
                    ),
                )
            )

            time_count = dim_time.count()

            logger.info(
                "dim_time rows read by Spark: %d",
                time_count
            )

            # =================================================
            # DEBUG JOIN COVERAGE
            # =================================================

            grid_matches = (
                hourly
                .join(
                    dim_grid,
                    on="grid_id",
                    how="inner",
                )
                .count()
            )

            logger.info(
                "Rows matching dim_grid: %d / %d",
                grid_matches,
                hourly_count
            )

            if grid_matches != hourly_count:

                logger.warning(
                    "Not every hourly row matched "
                    "dim_grid"
                )

            time_matches = (
                hourly
                .join(
                    dim_time,
                    on="timestamp",
                    how="inner",
                )
                .count()
            )

            logger.info(
                "Rows matching dim_time: %d / %d",
                time_matches,
                hourly_count
            )

            if time_matches != hourly_count:

                logger.warning(
                    "Not every hourly row matched "
                    "dim_time"
                )

            # =================================================
            # JOIN GRID
            # =================================================

            fact = (
                hourly.alias("h")
                .join(
                    dim_grid.alias("g"),
                    F.col(
                        "h.grid_id"
                    )
                    ==
                    F.col(
                        "g.grid_id"
                    ),
                    "inner",
                )
                .join(
                    dim_time.alias("t"),
                    F.col(
                        "h.timestamp"
                    )
                    ==
                    F.col(
                        "t.timestamp"
                    ),
                    "inner",
                )
                .select(
                    F.col(
                        "g.grid_key"
                    ).alias(
                        "grid_key"
                    ),

                    F.col(
                        "t.time_key"
                    ).alias(
                        "time_key"
                    ),

                    F.coalesce(
                        F.col(
                            "h.sms_in"
                        ),
                        F.lit(0.0),
                    ).alias(
                        "sms_in"
                    ),

                    F.coalesce(
                        F.col(
                            "h.sms_out"
                        ),
                        F.lit(0.0),
                    ).alias(
                        "sms_out"
                    ),

                    F.coalesce(
                        F.col(
                            "h.call_in"
                        ),
                        F.lit(0.0),
                    ).alias(
                        "call_in"
                    ),

                    F.coalesce(
                        F.col(
                            "h.call_out"
                        ),
                        F.lit(0.0),
                    ).alias(
                        "call_out"
                    ),

                    F.coalesce(
                        F.col(
                            "h.internet_activity"
                        ),
                        F.lit(0.0),
                    ).alias(
                        "internet_activity"
                    ),

                    F.coalesce(
                        F.col(
                            "h.total_sms"
                        ),
                        F.lit(0.0),
                    ).alias(
                        "total_sms"
                    ),

                    F.coalesce(
                        F.col(
                            "h.total_calls"
                        ),
                        F.lit(0.0),
                    ).alias(
                        "total_calls"
                    ),

                    F.coalesce(
                        F.col(
                            "h.total_activity"
                        ),
                        F.lit(0.0),
                    ).alias(
                        "total_activity"
                    ),
                )
            )

            # =================================================
            # FACT VALIDATION
            # =================================================

            fact_count = fact.count()

            logger.info(
                "Fact rows prepared: %d",
                fact_count
            )

            # -------------------------------------------------
            # DE6 acceptance criterion:
            # fact row count must equal hourly output
            # -------------------------------------------------

            if fact_count != hourly_count:

                raise ValueError(
                    "FACT ROW COUNT MISMATCH\n"
                    f"hourly_grid_summary: "
                    f"{hourly_count}\n"
                    f"fact rows after joins: "
                    f"{fact_count}\n"
                    "Check grid_id/timestamp "
                    "join coverage."
                )

            # -------------------------------------------------
            # Check fact grain
            # -------------------------------------------------

            duplicate_count = (
                fact
                .groupBy(
                    "grid_key",
                    "time_key",
                )
                .count()
                .filter(
                    F.col("count") > 1
                )
                .count()
            )

            if duplicate_count != 0:

                raise ValueError(
                    "Fact table grain violation: "
                    f"{duplicate_count} duplicate "
                    "(grid_key,time_key) groups"
                )

            logger.info(
                "Fact grain validation passed"
            )

            # =================================================
            # WRITE FACT TABLE
            # =================================================

            logger.info(
                "Writing fact_network_activity"
            )

            (
                fact.write
                .format("jdbc")
                .option(
                    "url",
                    JDBC_URL
                )
                .options(
                    **JDBC_PROPERTIES
                )
                .option(
                    "dbtable",
                    "fact_network_activity"
                )
                .option(
                    "batchsize",
                    "5000"
                )
                .option(
                    "numPartitions",
                    "4"
                )
                .mode(
                    "append"
                )
                .save()
            )

            logger.info(
                "Fact data loaded successfully"
            )

        finally:
            # DO NOT close a JDBC connection here and DO NOT stop Spark.
            # Spark/JDBC resources are owned by the universal session
            # and are cleaned up after the complete pipeline finishes.
            pass

    # ========================================================
    # VALIDATION
    # ========================================================

    def validate_warehouse(self):

        logger.info(
            "Running warehouse validation"
        )

        with self.engine.connect() as conn:

            grid_count = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM dim_grid
                    """
                )
            ).scalar()

            time_count = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM dim_time
                    """
                )
            ).scalar()

            fact_count = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM fact_network_activity
                    """
                )
            ).scalar()

            duplicate_grids = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT grid_id
                        FROM dim_grid
                        GROUP BY grid_id
                        HAVING COUNT(*) > 1
                    ) x
                    """
                )
            ).scalar()

            duplicate_facts = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT grid_key, time_key
                        FROM fact_network_activity
                        GROUP BY grid_key, time_key
                        HAVING COUNT(*) > 1
                    ) x
                    """
                )
            ).scalar()

        logger.info(
            "dim_grid count: %d",
            grid_count
        )

        logger.info(
            "dim_time count: %d",
            time_count
        )

        logger.info(
            "fact count: %d",
            fact_count
        )

        logger.info(
            "Duplicate grid IDs: %d",
            duplicate_grids
        )

        logger.info(
            "Duplicate fact grains: %d",
            duplicate_facts
        )

        assert (
            grid_count == 10000
        ), (
            "Expected 10,000 grid dimension "
            f"rows, got {grid_count}"
        )

        assert (
            duplicate_grids == 0
        )

        assert (
            duplicate_facts == 0
        )

        if fact_count == 0:

            raise ValueError(
                "fact_network_activity is empty"
            )

        logger.info(
            "Warehouse validation passed"
        )

    # ========================================================
    # RUN
    # ========================================================

    def run(self):

        try:
            self.connect()

            self.create_tables()

            self.load_grid_dimension()

            self.load_time_dimension()

            self.load_fact_with_spark()

            self.validate_warehouse()

            logger.info(
                "DE6 warehouse loading completed successfully"
            )

        finally:
            # One guaranteed cleanup point for the entire run.
            self.close()


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser()

    # Only the two input file paths come from the command line.
    parser.add_argument(
        "--hourly",
        required=True,
        help="Path to hourly_grid_summary Parquet output",
    )

    parser.add_argument(
        "--geojson",
        required=True,
        help="Path to milano-grid.geojson",
    )

    args = parser.parse_args()

    loader = WarehouseLoader(
        hourly_path=args.hourly,
        geojson_path=args.geojson,
    )

    loader.run()


if __name__ == "__main__":
    main()


# ============================================================
# EXAMPLE
# ============================================================
#
# python warehouse_loader.py `
#     --hourly D:\capstone1\data\processed\hourly_grid_summary_parquet `
#     --geojson D:\capstone1\data\reference\milano-grid.geojson
