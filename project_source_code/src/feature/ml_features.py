"""
ML2 - Network Activity Feature Engineering

Builds the persisted network_feature_table from the MySQL warehouse.

Feature convention
------------------
For a feature_timestamp t, every feature is calculated using data
available at or before t.

The resulting feature row is therefore suitable for predicting t+1.

Windows
-------
Recent window:
    t-23h ... t        (24 hourly observations)

Prior baseline:
    t-47h ... t-24h    (24 hourly observations)

Features
--------
avg_activity
activity_growth
active_hours
peak_ratio
variability
internet_share
"""

import logging
import os
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIGURATION
# ============================================================

JDBC_URL = os.getenv(
    "TELECOM_JDBC_URL",
    "jdbc:mysql://localhost:3306/telecom_analytics",
)

JDBC_PROPERTIES = {
    "user": os.getenv("TELECOM_DB_USER", "root"),
    "password": os.getenv("TELECOM_DB_PASSWORD", "root"),
    "driver": "com.mysql.cj.jdbc.Driver",
}

MYSQL_JDBC_JAR = os.getenv(
    "MYSQL_JDBC_JAR",
    r"D:\mysql-connector-j-26.7.0\mysql-connector-j-26.7.0.jar",
)

FEATURE_TABLE = "network_feature_table"

RECENT_WINDOW_HOURS = 24
PRIOR_WINDOW_HOURS = 24

REQUIRED_FEATURE_COLUMNS = [
    "grid_id",
    "feature_timestamp",
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("ml_features")


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session() -> SparkSession:
    """
    Create the Spark session used for ML2.
    """

    logger.info("Starting ML2 Spark session")

    spark = (
        SparkSession.builder
        .appName("ML2-NetworkFeatureEngineering")
        .config(
            "spark.driver.extraClassPath",
            MYSQL_JDBC_JAR,
        )
        .config(
            "spark.executor.extraClassPath",
            MYSQL_JDBC_JAR,
        )
        .getOrCreate()
    )

    logger.info("Spark session started")

    return spark


# ============================================================
# JDBC READER
# ============================================================

def read_mysql_table(
    spark: SparkSession,
    table_name: str,
) -> DataFrame:
    """
    Read a MySQL table using Spark JDBC.
    """

    logger.info(
        "Reading MySQL table: %s",
        table_name,
    )

    return (
        spark.read
        .format("jdbc")
        .option(
            "url",
            JDBC_URL,
        )
        .options(
            **JDBC_PROPERTIES,
        )
        .option(
            "dbtable",
            table_name,
        )
        .load()
    )


# ============================================================
# READ WAREHOUSE
# ============================================================

def read_warehouse_history(
    spark: SparkSession,
) -> DataFrame:
    """
    Construct the logical hourly grid history from:

        fact_network_activity
        dim_grid
        dim_time

    No Parquet or raw CSV is used here.
    """

    fact = (
        read_mysql_table(
            spark,
            "fact_network_activity",
        )
        .select(
            F.col("grid_key").cast("long"),
            F.col("time_key").cast("long"),
            F.col("total_sms").cast("double"),
            F.col("total_calls").cast("double"),
            F.col("internet_activity").cast("double"),
            F.col("total_activity").cast("double"),
        )
    )

    dim_grid = (
        read_mysql_table(
            spark,
            "dim_grid",
        )
        .select(
            F.col("grid_key").cast("long"),
            F.col("grid_id").cast("int"),
        )
    )

    dim_time = (
        read_mysql_table(
            spark,
            "dim_time",
        )
        .select(
            F.col("time_key").cast("long"),
            F.col("timestamp").cast("timestamp"),
        )
    )

    logger.info("Joining fact table with dimensions")

    history = (
        fact.alias("f")
        .join(
            dim_grid.alias("g"),
            F.col("f.grid_key") == F.col("g.grid_key"),
            "inner",
        )
        .join(
            dim_time.alias("t"),
            F.col("f.time_key") == F.col("t.time_key"),
            "inner",
        )
        .select(
            F.col("g.grid_id"),
            F.date_trunc(
                "hour",
                F.col("t.timestamp"),
            ).alias("timestamp"),

            F.coalesce(
                F.col("f.total_sms"),
                F.lit(0.0),
            ).alias("total_sms"),

            F.coalesce(
                F.col("f.total_calls"),
                F.lit(0.0),
            ).alias("total_calls"),

            F.coalesce(
                F.col("f.internet_activity"),
                F.lit(0.0),
            ).alias("internet_activity"),

            F.coalesce(
                F.col("f.total_activity"),
                F.lit(0.0),
            ).alias("total_activity"),
        )
    )

    return history


# ============================================================
# DATA VALIDATION
# ============================================================

def validate_required_columns(
    df: DataFrame,
) -> None:
    """
    Verify that the warehouse history contains all required columns.
    """

    required = {
        "grid_id",
        "timestamp",
        "total_sms",
        "total_calls",
        "internet_activity",
        "total_activity",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            "ML2 history is missing required columns: "
            f"{sorted(missing)}"
        )


def validate_grid_ids(
    df: DataFrame,
) -> None:
    """
    Validate the project grid range.
    """

    invalid_count = (
        df
        .filter(
            (F.col("grid_id") < 1)
            | (F.col("grid_id") > 10000)
        )
        .count()
    )

    if invalid_count > 0:
        raise ValueError(
            f"Found {invalid_count} rows with invalid grid_id"
        )

    logger.info(
        "Grid ID validation passed"
    )


def validate_duplicate_grain(
    df: DataFrame,
) -> None:
    """
    Ensure there is exactly one row per grid and hourly timestamp.
    """

    duplicate_groups = (
        df
        .groupBy(
            "grid_id",
            "timestamp",
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_groups > 0:
        raise ValueError(
            "ML2 grain violation: found "
            f"{duplicate_groups} duplicate "
            "(grid_id, timestamp) groups"
        )

    logger.info(
        "Hourly grid grain validation passed"
    )


def detect_cadence_gaps(
    df: DataFrame,
) -> DataFrame:
    """
    Detect non-hourly transitions without failing the pipeline.

    A gap is recorded when the difference between consecutive
    observations for a grid is not exactly one hour.

    We do NOT fill the missing observation with zero.

    Returns the original dataframe with:
        previous_timestamp
        hour_difference
        cadence_gap
    """

    logger.info("Checking hourly cadence")

    ordered = (
        Window
        .partitionBy("grid_id")
        .orderBy("timestamp")
    )

    checked = (
        df
        .withColumn(
            "previous_timestamp",
            F.lag("timestamp").over(ordered),
        )
        .withColumn(
            "hour_difference",
            (
                F.col("timestamp").cast("long")
                - F.col("previous_timestamp").cast("long")
            ) / 3600,
        )
        .withColumn(
            "cadence_gap",
            F.when(
                F.col("previous_timestamp").isNull(),
                F.lit(False),
            )
            .otherwise(
                F.col("hour_difference") != 1
            ),
        )
    )

    gap_count = (
        checked
        .filter(F.col("cadence_gap"))
        .count()
    )

    if gap_count == 0:
        logger.info(
            "Hourly cadence validation passed - no gaps found"
        )
    else:
        logger.warning(
            "Found %d non-hourly transitions. "
            "These will be handled as gaps rather than "
            "stopping ML2.",
            gap_count,
        )

        (
            checked
            .filter(F.col("cadence_gap"))
            .select(
                "grid_id",
                "previous_timestamp",
                "timestamp",
                "hour_difference",
            )
            .show(20, truncate=False)
        )

    return checked

# ============================================================
# BUILD CONTINUOUS HISTORY
# ============================================================

def prepare_history(
    history: DataFrame,
) -> DataFrame:
    """
    Prepare the warehouse history for feature engineering.
    """

    validate_required_columns(history)

    history = (
        history
        .filter(
            F.col("grid_id").isNotNull()
            & F.col("timestamp").isNotNull()
        )
        .withColumn(
            "total_sms",
            F.coalesce(
                F.col("total_sms"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "total_calls",
            F.coalesce(
                F.col("total_calls"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "internet_activity",
            F.coalesce(
                F.col("internet_activity"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "total_activity",
            F.coalesce(
                F.col("total_activity"),
                F.lit(0.0),
            ),
        )
    )

    validate_grid_ids(history)
    validate_duplicate_grain(history)
    history = detect_cadence_gaps(history)

    return history


# ============================================================
# FEATURE ENGINEERING
# ============================================================

def build_features(
    history: DataFrame,
) -> DataFrame:
    """
    Calculate the six approved ML2 features.

    Recent window:
        t-23h ... t

    Prior window:
        t-47h ... t-24h

    A feature is produced only when all 48 required hourly
    observations exist continuously.

    feature_timestamp = t
    """

    logger.info(
        "Building ML2 features"
    )

    # --------------------------------------------------------
    # Create a continuous sequence identifier.
    #
    # Whenever there is a cadence gap, start a new sequence.
    # --------------------------------------------------------

    ordered = (
        Window
        .partitionBy("grid_id")
        .orderBy("timestamp")
    )

    history = (
        history
        .withColumn(
            "_new_sequence",
            F.when(
                F.col("previous_timestamp").isNull()
                | F.col("cadence_gap"),
                F.lit(1),
            ).otherwise(
                F.lit(0)
            ),
        )
        .withColumn(
            "_sequence_id",
            F.sum("_new_sequence").over(
                ordered
            ),
        )
    )

    # --------------------------------------------------------
    # Now windows are calculated INSIDE each continuous
    # sequence.
    #
    # This prevents a window from jumping across a missing hour.
    # --------------------------------------------------------

    window_recent = (
        Window
        .partitionBy(
            "grid_id",
            "_sequence_id",
        )
        .orderBy("timestamp")
        .rowsBetween(
            -23,
            0,
        )
    )

    window_prior = (
        Window
        .partitionBy(
            "grid_id",
            "_sequence_id",
        )
        .orderBy("timestamp")
        .rowsBetween(
            -47,
            -24,
        )
    )

    history_count_window = (
        Window
        .partitionBy(
            "grid_id",
            "_sequence_id",
        )
        .orderBy("timestamp")
        .rowsBetween(
            -47,
            0,
        )
    )

    # --------------------------------------------------------
    # FEATURE CALCULATIONS
    # --------------------------------------------------------

    features = (
        history

        # ====================================================
        # Recent 24 hours
        # ====================================================

        .withColumn(
            "recent_avg",
            F.avg(
                "total_activity"
            ).over(
                window_recent
            ),
        )

        .withColumn(
            "recent_peak",
            F.max(
                "total_activity"
            ).over(
                window_recent
            ),
        )

        .withColumn(
            "recent_active_hours",
            F.sum(
                F.when(
                    F.col("total_activity") > 0,
                    1,
                ).otherwise(
                    0
                )
            ).over(
                window_recent
            ),
        )

        .withColumn(
            "recent_stddev",
            F.stddev_samp(
                "total_activity"
            ).over(
                window_recent
            ),
        )

        .withColumn(
            "recent_internet_activity",
            F.sum(
                "internet_activity"
            ).over(
                window_recent
            ),
        )

        .withColumn(
            "recent_total_activity",
            F.sum(
                "total_activity"
            ).over(
                window_recent
            ),
        )

        # ====================================================
        # Previous 24 hours
        # ====================================================

        .withColumn(
            "prior_avg",
            F.avg(
                "total_activity"
            ).over(
                window_prior
            ),
        )

        # ====================================================
        # Final features
        # ====================================================

        .withColumn(
            "avg_activity",
            F.col("recent_avg"),
        )

        .withColumn(
            "activity_growth",
            F.when(
                F.col("prior_avg").isNull(),
                F.lit(None).cast("double"),
            )
            .when(
                F.col("prior_avg") == 0,
                F.when(
                    F.col("recent_avg") == 0,
                    F.lit(0.0),
                ).otherwise(
                    F.lit(1.0),
                ),
            )
            .otherwise(
                (
                    F.col("recent_avg")
                    - F.col("prior_avg")
                )
                / F.col("prior_avg")
            ),
        )

        .withColumn(
            "active_hours",
            F.col(
                "recent_active_hours"
            ).cast("int"),
        )

        .withColumn(
            "peak_ratio",
            F.when(
                F.col("recent_avg").isNull(),
                F.lit(None).cast("double"),
            )
            .when(
                F.col("recent_avg") == 0,
                F.lit(0.0),
            )
            .otherwise(
                F.col("recent_peak")
                / F.col("recent_avg")
            ),
        )

        .withColumn(
            "variability",
            F.when(
                F.col("recent_stddev").isNull(),
                F.lit(0.0),
            ).otherwise(
                F.col("recent_stddev")
            ),
        )

        .withColumn(
            "internet_share",
            F.when(
                F.col("recent_total_activity") == 0,
                F.lit(0.0),
            )
            .otherwise(
                F.col("recent_internet_activity")
                / F.col("recent_total_activity")
            ),
        )

        # ====================================================
        # Require complete 48-hour history
        # ====================================================

        .withColumn(
            "_history_count",
            F.count(
                F.lit(1)
            ).over(
                history_count_window
            ),
        )

        .filter(
            F.col("_history_count") == 48
        )

        # ----------------------------------------------------
        # Final ML2 schema
        # ----------------------------------------------------

        .select(
            F.col("grid_id").cast("int"),
            F.col("timestamp").alias(
                "feature_timestamp"
            ),
            F.col("avg_activity").cast("double"),
            F.col("activity_growth").cast("double"),
            F.col("active_hours").cast("int"),
            F.col("peak_ratio").cast("double"),
            F.col("variability").cast("double"),
            F.col("internet_share").cast("double"),
        )
    )

    return features


# ============================================================
# FEATURE VALIDATION
# ============================================================

def validate_features(
    features: DataFrame,
) -> None:
    """
    Validate the persisted ML2 feature contract.
    """

    logger.info(
        "Validating ML2 feature table"
    )

    actual_columns = features.columns

    if actual_columns != REQUIRED_FEATURE_COLUMNS:
        raise ValueError(
            "ML2 feature schema mismatch.\n"
            f"Expected: {REQUIRED_FEATURE_COLUMNS}\n"
            f"Actual:   {actual_columns}"
        )

    # --------------------------------------------------------
    # Null checks
    # --------------------------------------------------------

    null_condition = (
        F.col("grid_id").isNull()
        | F.col("feature_timestamp").isNull()
        | F.col("avg_activity").isNull()
        | F.col("activity_growth").isNull()
        | F.col("active_hours").isNull()
        | F.col("peak_ratio").isNull()
        | F.col("variability").isNull()
        | F.col("internet_share").isNull()
    )

    null_count = (
        features
        .filter(null_condition)
        .count()
    )

    if null_count > 0:
        raise ValueError(
            f"ML2 feature table contains {null_count} "
            "rows with NULL feature values"
        )

    # --------------------------------------------------------
    # Infinite / NaN checks
    # --------------------------------------------------------

    invalid_numeric = (
        F.isnan(F.col("avg_activity"))
        | F.isnan(F.col("activity_growth"))
        | F.isnan(F.col("peak_ratio"))
        | F.isnan(F.col("variability"))
        | F.isnan(F.col("internet_share"))
        | (F.abs(F.col("activity_growth")) == float("inf"))
        | (F.abs(F.col("peak_ratio")) == float("inf"))
        | (F.abs(F.col("internet_share")) == float("inf"))
    )

    invalid_count = (
        features
        .filter(invalid_numeric)
        .count()
    )

    if invalid_count > 0:
        raise ValueError(
            f"Found {invalid_count} invalid "
            "NaN/infinite feature rows"
        )

    # --------------------------------------------------------
    # active_hours range
    # --------------------------------------------------------

    invalid_active_hours = (
        features
        .filter(
            (F.col("active_hours") < 0)
            | (F.col("active_hours") > 24)
        )
        .count()
    )

    if invalid_active_hours > 0:
        raise ValueError(
            "active_hours must be between 0 and 24"
        )

    # --------------------------------------------------------
    # internet_share range
    # --------------------------------------------------------

    invalid_share = (
        features
        .filter(
            (F.col("internet_share") < 0)
            | (F.col("internet_share") > 1)
        )
        .count()
    )

    if invalid_share > 0:
        raise ValueError(
            "internet_share must be between 0 and 1"
        )

    logger.info(
        "Feature validation passed"
    )


# ============================================================
# CREATE FEATURE TABLE
# ============================================================

def create_feature_table(
    spark: SparkSession,
) -> None:
    """
    Create the MySQL destination table if it does not exist.
    """

    logger.info(
        "Creating %s if required",
        FEATURE_TABLE,
    )

    ddl = """
    CREATE TABLE IF NOT EXISTS network_feature_table (
        grid_id INT NOT NULL,
        feature_timestamp DATETIME NOT NULL,

        avg_activity DOUBLE NOT NULL,
        activity_growth DOUBLE NOT NULL,
        active_hours INT NOT NULL,
        peak_ratio DOUBLE NOT NULL,
        variability DOUBLE NOT NULL,
        internet_share DOUBLE NOT NULL,

        PRIMARY KEY (
            grid_id,
            feature_timestamp
        ),

        INDEX idx_feature_timestamp (
            feature_timestamp
        ),

        INDEX idx_feature_grid (
            grid_id
        )
    )
    """

    (
        spark.read
        .format("jdbc")
        .option(
            "url",
            JDBC_URL,
        )
        .options(
            **JDBC_PROPERTIES,
        )
        .option(
            "query",
            "SELECT 1",
        )
        .load()
    )

    # DDL is easier and safer through the JDBC connection.
    java_gateway = spark.sparkContext._gateway
    jvm = java_gateway.jvm

    conn = None
    statement = None

    try:
        conn = jvm.java.sql.DriverManager.getConnection(
            JDBC_URL,
            JDBC_PROPERTIES["user"],
            JDBC_PROPERTIES["password"],
        )

        statement = conn.createStatement()
        statement.execute(ddl)

    finally:
        if statement is not None:
            statement.close()

        if conn is not None:
            conn.close()

    logger.info(
        "%s is ready",
        FEATURE_TABLE,
    )


# ============================================================
# WRITE FEATURE TABLE
# ============================================================

def write_feature_table(
    features: DataFrame,
) -> None:
    """
    Replace the current feature table with the newly generated
    reproducible feature output.
    """

    logger.info(
        "Writing ML2 features to %s",
        FEATURE_TABLE,
    )

    (
        features.write
        .format("jdbc")
        .option(
            "url",
            JDBC_URL,
        )
        .options(
            **JDBC_PROPERTIES,
        )
        .option(
            "dbtable",
            FEATURE_TABLE,
        )
        .option(
            "batchsize",
            "5000",
        )
        .option(
            "numPartitions",
            "4",
        )
        .mode(
            "overwrite",
        )
        .save()
    )

    logger.info(
        "ML2 feature table written successfully"
    )


# ============================================================
# LEAKAGE TEST
# ============================================================

def run_leakage_test(
    history: DataFrame,
    features: DataFrame,
) -> None:
    """
    Verify the ML2 temporal boundary.

    For every feature row at timestamp t, the source rows used
    to construct it must not contain any timestamp > t.

    The real implementation should pass this test.
    """

    logger.info(
        "Running ML2 leakage test"
    )

    max_source_timestamp = (
        history
        .groupBy("grid_id")
        .agg(
            F.max("timestamp").alias(
                "max_source_timestamp"
            )
        )
    )

    checked = (
        features
        .join(
            max_source_timestamp,
            on="grid_id",
            how="left",
        )
    )

    # This alone checks the global dataset boundary.
    # The stronger test below constructs the maximum source
    # timestamp inside the actual feature window.

    recent_window = (
        Window
        .partitionBy("grid_id")
        .orderBy("timestamp")
        .rowsBetween(
            -23,
            0,
        )
    )

    source_with_feature_boundary = (
        history
        .withColumn(
            "_feature_timestamp",
            F.last("timestamp").over(
                recent_window
            ),
        )
    )

    leakage_count = (
        source_with_feature_boundary
        .filter(
            F.col("timestamp")
            > F.col("_feature_timestamp")
        )
        .count()
    )

    if leakage_count != 0:
        raise AssertionError(
            "LEAKAGE TEST FAILED: "
            f"{leakage_count} source rows occur after "
            "their feature_timestamp"
        )

    # --------------------------------------------------------
    # Final persisted feature boundary check
    # --------------------------------------------------------

    future_join = (
        history.alias("h")
        .join(
            features.alias("f"),
            F.col("h.grid_id")
            == F.col("f.grid_id"),
            "inner",
        )
        .filter(
            F.col("h.timestamp")
            > F.col("f.feature_timestamp")
        )
    )

    future_count = future_join.count()

    if future_count != 0:
        raise AssertionError(
            "LEAKAGE TEST FAILED: "
            f"{future_count} future source rows found "
            "after feature_timestamp"
        )

    logger.info(
        "ML2 leakage test PASSED"
    )


# ============================================================
# MAIN PIPELINE
# ============================================================

def build_and_persist_features(
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """
    Complete ML2 feature engineering pipeline.
    """

    owns_spark = spark is None

    if spark is None:
        spark = create_spark_session()

    try:
        # ----------------------------------------------------
        # 1. Read warehouse
        # ----------------------------------------------------

        history = read_warehouse_history(
            spark
        )

        logger.info(
            "Warehouse history loaded"
        )

        # ----------------------------------------------------
        # 2. Validate history
        # ----------------------------------------------------

        history = prepare_history(
            history
        )

        # ----------------------------------------------------
        # 3. Build six features
        # ----------------------------------------------------

        features = build_features(
            history
        )

        feature_count = features.count()

        logger.info(
            "Feature rows generated: %d",
            feature_count,
        )

        if feature_count == 0:
            raise ValueError(
                "ML2 generated zero feature rows. "
                "At least 48 hours of valid history is required."
            )

        # ----------------------------------------------------
        # 4. Validate feature output
        # ----------------------------------------------------

        validate_features(
            features
        )

        # ----------------------------------------------------
        # 5. Leakage test
        # ----------------------------------------------------

        run_leakage_test(
            history,
            features,
        )

        # ----------------------------------------------------
        # 6. Create destination
        # ----------------------------------------------------

        create_feature_table(
            spark
        )

        # ----------------------------------------------------
        # 7. Persist
        # ----------------------------------------------------

        write_feature_table(
            features
        )

        # ----------------------------------------------------
        # 8. Final count
        # ----------------------------------------------------

        persisted_count = (
            spark.read
            .format("jdbc")
            .option(
                "url",
                JDBC_URL,
            )
            .options(
                **JDBC_PROPERTIES,
            )
            .option(
                "dbtable",
                FEATURE_TABLE,
            )
            .load()
            .count()
        )

        logger.info(
            "Persisted feature rows: %d",
            persisted_count,
        )

        if persisted_count != feature_count:
            raise ValueError(
                "Persisted feature row count does not "
                "match generated feature row count"
            )

        logger.info(
            "ML2 feature engineering completed successfully"
        )

        return features

    finally:
        if owns_spark:
            logger.info(
                "Stopping ML2 Spark session"
            )
            spark.stop()


# ============================================================
# SCRIPT ENTRY POINT
# ============================================================

if __name__ == "__main__":

    build_and_persist_features()


