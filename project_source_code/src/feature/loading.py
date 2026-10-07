from datetime import datetime, timedelta
"""
ML2 — Engineer Network Activity Features
=========================================
 
Input:
    MySQL capstone1 database
        dim_time
        dim_grid
        fact_network_activity
 
Output:
    MySQL table:
        network_feature_table
 
Prediction convention:
    feature_timestamp = t
 
    Features may use:
        trailing 24 hours ending at t
 
    Features MUST NOT use:
        anything after t
 
Features:
    avg_activity
    activity_growth
    active_hours
    peak_ratio
    variability
    internet_share
 
Leakage prevention:
    Spark window frames are explicitly bounded so that
    future rows cannot enter the feature calculation.
"""
 
import os
import sys
import logging
from datetime import datetime
 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    IntegerType
)
 
import os
os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["PATH"] = (
    r"D:\hadoop\bin"
    + os.pathsep
    + os.environ.get("PATH", "")
)

# ============================================================
# CONFIGURATION
# ============================================================
 
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "telecom_analytics"
DB_USER = "root"
DB_PASSWORD = "root"
 
MYSQL_JAR = r"D:\mysql-connector-j-26.7.0/mysql-connector-j-26.7.0.jar"
 
OUTPUT_TABLE = "network_feature_table1"
 
RECENT_WINDOW_HOURS = 24
BASELINE_WINDOW_HOURS = 24
 
 
# ============================================================
# LOGGING
# ============================================================
 
LOG_DIR = "D:/capstone1/log"
os.makedirs(LOG_DIR, exist_ok=True)
 
LOG_FILE = os.path.join(
    LOG_DIR,
    "ml2_features.log"
)
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
 
logger = logging.getLogger("ML2")
 
 
# ============================================================
# SPARK SESSION
# ============================================================
 
def create_spark_session():
 
    logger.info("Creating SparkSession")
 
    spark = (
        SparkSession.builder
        .appName("ML2-Network-Feature-Engineering")
        .master("local[*]")
        .config(
            "spark.jars",
            MYSQL_JAR
        )
        .config(
            "spark.sql.shuffle.partitions",
            "200"
        )
        .getOrCreate()
    )
 
    spark.sparkContext.setLogLevel("WARN")
 
    logger.info("SparkSession created")
 
    return spark
 
 
# ============================================================
# JDBC CONFIGURATION
# ============================================================
 
def jdbc_properties():
 
    return {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "com.mysql.cj.jdbc.Driver",
        "fetchsize": "50000"
    }
 
 
def jdbc_url():
 
    return (
        f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
        f"?useSSL=false"
        f"&allowPublicKeyRetrieval=true"
        f"&serverTimezone=UTC"
    )
 
 
# ============================================================
# READ DATABASE
# ============================================================
 
def read_source_data(spark):
 
    logger.info("Reading network activity from MySQL")
 
    query = """
    (
        SELECT
            f.grid_key,
            g.grid_id,
            t.timestamp,
            f.internet_activity,
            f.total_activity
        FROM fact_network_activity f
        INNER JOIN dim_time t
            ON f.time_key = t.time_key
        INNER JOIN dim_grid g
            ON f.grid_key = g.grid_key
    ) AS source_data
    """
 
    df = (
        spark.read
        .jdbc(
            url=jdbc_url(),
            table=query,
            properties=jdbc_properties()
        )
    )
 
    logger.info(
        "Source columns: %s",
        df.columns
    )
 
    row_count = df.count()
 
    logger.info(
        "Input rows: %d",
        row_count
    )
 
    if row_count == 0:
        raise RuntimeError(
            "Source table contains no network activity rows"
        )
 
    return df
 
 
# ============================================================
# CLEAN SOURCE
# ============================================================
 
def clean_source(df):
 
    logger.info("Cleaning source data")
 
    df = (
        df
        .withColumn(
            "timestamp",
            F.to_timestamp("timestamp")
        )
        .withColumn(
            "total_activity",
            F.coalesce(
                F.col("total_activity").cast("double"),
                F.lit(0.0)
            )
        )
        .withColumn(
            "internet_activity",
            F.coalesce(
                F.col("internet_activity").cast("double"),
                F.lit(0.0)
            )
        )
    )
 
    # Remove impossible negative values.
    # We do not allow them into feature calculations.
    df = df.filter(
        (F.col("total_activity") >= 0)
        &
        (F.col("internet_activity") >= 0)
    )
 
    # Remove rows without grid/timestamp.
    df = df.filter(
        F.col("grid_id").isNotNull()
        &
        F.col("timestamp").isNotNull()
    )
 
    logger.info(
        "Source cleaning completed"
    )
 
    return df
 
 
# ============================================================
# REMOVE DUPLICATES
# ============================================================
 
def remove_duplicates(df):
 
    logger.info(
        "Checking duplicate grid/timestamp records"
    )
 
    before = df.count()
 
    df = df.dropDuplicates(
        ["grid_id", "timestamp"]
    )
 
    after = df.count()
 
    removed = before - after
 
    logger.info(
        "Duplicate rows removed: %d",
        removed
    )
 
    return df
 
 
# ============================================================
# BUILD FEATURES
# ============================================================
 
def build_features(df):
 
    logger.info("Building ML2 network activity features")
 
    # ---------------------------------------------------------
    # Window definition
    # ---------------------------------------------------------
    feature_window = (
        Window
        .partitionBy("grid_id")
        .orderBy("timestamp")
        .rowsBetween(-(RECENT_WINDOW_HOURS - 1), 0)
    )
 
    baseline_window = (
        Window
        .partitionBy("grid_id")
        .orderBy("timestamp")
        .rowsBetween(
            -(RECENT_WINDOW_HOURS + BASELINE_WINDOW_HOURS - 1),
            -RECENT_WINDOW_HOURS
        )
    )
 
    # ---------------------------------------------------------
    # Recent 24-hour features
    # ---------------------------------------------------------
 
    df = (
        df
        .withColumn(
            "avg_activity",
            F.avg("total_activity").over(feature_window)
        )
        .withColumn(
            "_peak_activity",
            F.max("total_activity").over(feature_window)
        )
        .withColumn(
            "active_hours",
            F.sum(
                F.when(F.col("total_activity") > 0, 1)
                 .otherwise(0)
            ).over(feature_window)
        )
        .withColumn(
            "variability",
            F.stddev_pop("total_activity").over(feature_window)
        )
        .withColumn(
            "_recent_total_activity",
            F.sum("total_activity").over(feature_window)
        )
        .withColumn(
            "_recent_internet_activity",
            F.sum("internet_activity").over(feature_window)
        )
        .withColumn(
            "_baseline_avg",
            F.avg("total_activity").over(baseline_window)
        )
    )
 
    # ---------------------------------------------------------
    # Derived features
    # ---------------------------------------------------------
 
    df = (
        df
        .withColumn(
            "activity_growth",
            F.when(
                F.col("_baseline_avg").isNull() |
                (F.col("_baseline_avg") == 0),
                F.lit(0.0)
            ).otherwise(
                (
                    F.col("avg_activity") -
                    F.col("_baseline_avg")
                ) / F.col("_baseline_avg")
            )
        )
        .withColumn(
            "peak_ratio",
            F.when(
                F.col("avg_activity").isNull() |
                (F.col("avg_activity") == 0),
                F.lit(0.0)
            ).otherwise(
                F.col("_peak_activity") /
                F.col("avg_activity")
            )
        )
        .withColumn(
            "internet_share",
            F.when(
                F.col("_recent_total_activity") == 0,
                F.lit(0.0)
            ).otherwise(
                F.col("_recent_internet_activity") /
                F.col("_recent_total_activity")
            )
        )
        .withColumn(
            "variability",
            F.coalesce(F.col("variability"), F.lit(0.0))
        )
    )
 
    # ---------------------------------------------------------
    # IMPORTANT:
    # Keep timestamp here so we can verify 48 hours of history
    # ---------------------------------------------------------
 
    history_window = (
        Window
        .partitionBy("grid_id")
        .orderBy("timestamp")
        .rowsBetween(-47, 0)
    )
 
    df = df.withColumn(
        "_history_count",
        F.count("*").over(history_window)
    )
 
    # Only keep rows with complete 48-hour history
    df = df.filter(
        F.col("_history_count") >= 48
    )
 
    # ---------------------------------------------------------
    # feature_timestamp = t
    # ---------------------------------------------------------
 
    df = df.withColumn(
        "feature_timestamp",
        F.col("timestamp")
    )
 
    # ---------------------------------------------------------
    # Final ML2 feature table
    # ---------------------------------------------------------
 
    features = df.select(
        "grid_id",
        "feature_timestamp",
        "avg_activity",
        "activity_growth",
        "active_hours",
        "peak_ratio",
        "variability",
        "internet_share"
    )
 
    logger.info("ML2 feature columns:")
    logger.info(str(features.columns))
 
    return features
 
# ============================================================
# VALIDATE FEATURES
# ============================================================
 
def validate_features(features):
 
    logger.info(
        "Starting feature validation"
    )
 
    required_columns = [
        "grid_id",
        "feature_timestamp",
        "avg_activity",
        "activity_growth",
        "active_hours",
        "peak_ratio",
        "variability",
        "internet_share"
    ]
 
    # --------------------------------------------------------
    # Exact column names
    # --------------------------------------------------------
 
    expected_columns = [
        "grid_id",
        "feature_timestamp",
        "avg_activity",
        "activity_growth",
        "active_hours",
        "peak_ratio",
        "variability",
        "internet_share"
    ]
 
    if features.columns != expected_columns:
        raise AssertionError(
            f"Column mismatch.\n"
            f"Expected: {expected_columns}\n"
            f"Actual:   {features.columns}"
        )
 
    logger.info(
        "PASS: six feature names match exactly"
    )
 
    # --------------------------------------------------------
    # Null checks
    # --------------------------------------------------------
 
    numeric_columns = [
        "avg_activity",
        "activity_growth",
        "active_hours",
        "peak_ratio",
        "variability",
        "internet_share"
    ]
 
    null_condition = None
 
    for column in numeric_columns:
 
        condition = F.col(column).isNull()
 
        if null_condition is None:
            null_condition = condition
        else:
            null_condition = (
                null_condition | condition
            )
 
    null_count = (
        features
        .filter(null_condition)
        .count()
    )
 
    if null_count > 0:
        raise AssertionError(
            f"Found {null_count} rows with NULL features"
        )
 
    logger.info(
        "PASS: no NULL feature values"
    )
 
    # --------------------------------------------------------
    # Infinite values
    # --------------------------------------------------------
 
    infinite_condition = None
 
    for column in numeric_columns:
 
        condition = (
            F.isnan(F.col(column))
            |
            (F.abs(F.col(column)) == float("inf"))
        )
 
        if infinite_condition is None:
            infinite_condition = condition
        else:
            infinite_condition = (
                infinite_condition | condition
            )
 
    infinite_count = (
        features
        .filter(infinite_condition)
        .count()
    )
 
    if infinite_count > 0:
        raise AssertionError(
            f"Found {infinite_count} "
            f"rows with NaN/infinite values"
        )
 
    logger.info(
        "PASS: no NaN/infinite values"
    )
 
    # --------------------------------------------------------
    # Active hours
    # --------------------------------------------------------
 
    invalid_active_hours = (
        features
        .filter(
            (F.col("active_hours") < 0)
            |
            (F.col("active_hours") > 24)
        )
        .count()
    )
 
    if invalid_active_hours > 0:
        raise AssertionError(
            "active_hours outside 0-24"
        )
 
    logger.info(
        "PASS: active_hours within 0-24"
    )
 
    # --------------------------------------------------------
    # Internet share
    # --------------------------------------------------------
 
    invalid_share = (
        features
        .filter(
            (F.col("internet_share") < 0)
            |
            (F.col("internet_share") > 1)
        )
        .count()
    )
 
    if invalid_share > 0:
        raise AssertionError(
            "internet_share outside 0-1"
        )
 
    logger.info(
        "PASS: internet_share within 0-1"
    )
 
    # --------------------------------------------------------
    # Timestamp
    # --------------------------------------------------------
 
    missing_timestamp = (
        features
        .filter(
            F.col("feature_timestamp").isNull()
        )
        .count()
    )
 
    if missing_timestamp > 0:
        raise AssertionError(
            "feature_timestamp contains NULL"
        )
 
    logger.info(
        "PASS: every row has feature_timestamp"
    )
 
    logger.info(
        "Feature validation PASSED"
    )
 
 
# ============================================================
# HAND CHECK
# ============================================================
 
def hand_check(source_df, feature_df):
 
    logger.info(
        "Starting hand-check for one grid"
    )
 
    # Get one grid/timestamp where features exist.
    sample = (
        feature_df
        .orderBy("grid_id", "feature_timestamp")
        .limit(1)
        .collect()
    )
 
    if not sample:
        raise RuntimeError(
            "No feature rows available for hand-check"
        )
 
    row = sample[0]
 
    grid_id = row["grid_id"]
    feature_timestamp = row[
        "feature_timestamp"
    ]
 
    # --------------------------------------------------------
    # Retrieve exactly the trailing 24 records.
    # --------------------------------------------------------
 
    recent = (
        source_df
        .filter(
            F.col("grid_id") == grid_id
        )
        .filter(
            F.col("timestamp")
            <= feature_timestamp
        )
        .orderBy(
            F.col("timestamp").desc()
        )
        .limit(RECENT_WINDOW_HOURS)
    )
 
    values = [
        r["total_activity"]
        for r in recent.collect()
    ]
 
    if len(values) != RECENT_WINDOW_HOURS:
        logger.warning(
            "Skipping hand-check because "
            "sample does not contain full window"
        )
        return
 
    manual_avg = sum(values) / len(values)
 
    manual_peak = max(values)
 
    if manual_avg == 0:
        manual_peak_ratio = 0.0
    else:
        manual_peak_ratio = (
            manual_peak / manual_avg
        )
 
    spark_row = (
        feature_df
        .filter(
            (F.col("grid_id") == grid_id)
            &
            (
                F.col("feature_timestamp")
                == feature_timestamp
            )
        )
        .first()
    )
 
    logger.info(
        "Hand-check grid=%s timestamp=%s",
        grid_id,
        feature_timestamp
    )
 
    logger.info(
        "Manual avg_activity=%s",
        manual_avg
    )
 
    logger.info(
        "Spark avg_activity=%s",
        spark_row["avg_activity"]
    )
 
    logger.info(
        "Manual peak_ratio=%s",
        manual_peak_ratio
    )
 
    logger.info(
        "Spark peak_ratio=%s",
        spark_row["peak_ratio"]
    )
 
    assert abs(
        manual_avg
        - spark_row["avg_activity"]
    ) < 1e-9
 
    assert abs(
        manual_peak_ratio
        - spark_row["peak_ratio"]
    ) < 1e-9
 
    logger.info(
        "PASS: hand-check reproduced "
        "avg_activity and peak_ratio"
    )
 
 
# ============================================================
# LEAKAGE TEST
# ============================================================
 
def leakage_test(spark):
 
    logger.info("==========================================")
    logger.info("Starting leakage test")
 
    # ---------------------------------------------------------
    # Controlled dataset
    #
    # We need at least 48 historical rows because
    # build_features() requires complete 48-hour history.
    #
    # t = 2026-01-03 00:00
    # t+1 = 2026-01-03 01:00
    # ---------------------------------------------------------
 
    schema = StructType([
        StructField("grid_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("internet_activity", DoubleType(), False),
        StructField("total_activity", DoubleType(), False)
    ])
 
    # 48 hours of historical data
    data_before = []
 
    start_time = datetime(2026, 1, 1, 0, 0)
 
    for i in range(48):
        ts = start_time + timedelta(hours=i)
 
        data_before.append(
            (
                "TEST_GRID",
                ts,
                10.0 + i,
                100.0 + (i * 10)
            )
        )
 
    # ---------------------------------------------------------
    # Future row at t+1
    # ---------------------------------------------------------
 
    future_row = (
        "TEST_GRID",
        datetime(2026, 1, 3, 1, 0),
        9999.0,
        999999.0
    )
 
    data_after = data_before + [future_row]
 
    # ---------------------------------------------------------
    # Create Spark DataFrames
    # ---------------------------------------------------------
 
    df_before = spark.createDataFrame(
        data_before,
        schema
    )
 
    df_after = spark.createDataFrame(
        data_after,
        schema
    )
 
    # ---------------------------------------------------------
    # Controlled window configuration
    # ---------------------------------------------------------
 
    global RECENT_WINDOW_HOURS
    global BASELINE_WINDOW_HOURS
 
    old_recent = RECENT_WINDOW_HOURS
    old_baseline = BASELINE_WINDOW_HOURS
 
    RECENT_WINDOW_HOURS = 2
    BASELINE_WINDOW_HOURS = 1
 
    try:
 
        # -----------------------------------------------------
        # Build features before future data
        # -----------------------------------------------------
 
        features_before = build_features(
            df_before
        )
 
        # -----------------------------------------------------
        # Build features after future data is added
        # -----------------------------------------------------
 
        features_after = build_features(
            df_after
        )
 
        # -----------------------------------------------------
        # t is the LAST historical timestamp.
        #
        # Future data is at t+1.
        # -----------------------------------------------------
 
        t = datetime(
            2026, 1, 3, 0, 0
        )
 
        # -----------------------------------------------------
        # Find feature row at t
        # -----------------------------------------------------
 
        row_before = (
            features_before
            .filter(
                F.col("feature_timestamp") == F.lit(t)
            )
            .first()
        )
 
        row_after = (
            features_after
            .filter(
                F.col("feature_timestamp") == F.lit(t)
            )
            .first()
        )
 
        # -----------------------------------------------------
        # Validate rows exist
        # -----------------------------------------------------
 
        if row_before is None:
            logger.error(
                "Available timestamps BEFORE future data:"
            )
 
            features_before.select(
                "feature_timestamp"
            ).orderBy(
                "feature_timestamp"
            ).show(10, truncate=False)
 
            raise AssertionError(
                "Leakage test could not find t row"
            )
 
        if row_after is None:
            logger.error(
                "Available timestamps AFTER future data:"
            )
 
            features_after.select(
                "feature_timestamp"
            ).orderBy(
                "feature_timestamp"
            ).show(10, truncate=False)
 
            raise AssertionError(
                "Leakage test could not find t row "
                "after adding future data"
            )
 
        # -----------------------------------------------------
        # Features that must remain unchanged
        # -----------------------------------------------------
 
        feature_columns = [
            "avg_activity",
            "activity_growth",
            "active_hours",
            "peak_ratio",
            "variability",
            "internet_share"
        ]
 
        # -----------------------------------------------------
        # Compare features
        # -----------------------------------------------------
 
        for column in feature_columns:
 
            before_value = row_before[column]
            after_value = row_after[column]
 
            if before_value is None:
                before_value = 0.0
 
            if after_value is None:
                after_value = 0.0
 
            difference = abs(
                before_value - after_value
            )
 
            logger.info(
                f"{column}: "
                f"before={before_value}, "
                f"after={after_value}, "
                f"difference={difference}"
            )
 
            assert difference < 1e-9, (
                f"LEAKAGE DETECTED: {column} "
                f"changed after adding t+1"
            )
 
        # -----------------------------------------------------
        # Test passed
        # -----------------------------------------------------
 
        logger.info(
            "PASS: future t+1 data does not "
            "change features at t"
        )
 
        logger.info(
            "Leakage test PASSED"
        )
 
    finally:
 
        # Restore original configuration
        RECENT_WINDOW_HOURS = old_recent
        BASELINE_WINDOW_HOURS = old_baseline
 
 
# ============================================================
# BROKEN IMPLEMENTATION TEST
# ============================================================
 
def demonstrate_broken_implementation(spark):
 
    logger.info(
        "Starting deliberately broken leakage demonstration"
    )
 
    # --------------------------------------------------------
    # Here we intentionally include t+1.
    #
    # This is NOT the real implementation.
    # --------------------------------------------------------
 
    data = [
        (
            "TEST_GRID",
            datetime(2026, 1, 1, 0, 0),
            10.0,
            100.0
        ),
        (
            "TEST_GRID",
            datetime(2026, 1, 1, 1, 0),
            20.0,
            200.0
        ),
        (
            "TEST_GRID",
            datetime(2026, 1, 1, 2, 0),
            30.0,
            300.0
        ),
        (
            "TEST_GRID",
            datetime(2026, 1, 1, 3, 0),
            9999.0,
            999999.0
        )
    ]
 
    schema = StructType([
        StructField(
            "grid_id",
            StringType(),
            False
        ),
        StructField(
            "timestamp",
            TimestampType(),
            False
        ),
        StructField(
            "internet_activity",
            DoubleType(),
            False
        ),
        StructField(
            "total_activity",
            DoubleType(),
            False
        )
    ])
 
    df = spark.createDataFrame(
        data,
        schema
    )
 
    t = datetime(
        2026, 1, 1, 2, 0
    )
 
    # --------------------------------------------------------
    # Correct:
    #
    # t-1, t
    # --------------------------------------------------------
 
    correct_avg = (
        df
        .filter(
            (F.col("timestamp") >= datetime(2026, 1, 1, 1, 0))
            &
            (F.col("timestamp") <= t)
        )
        .agg(
            F.avg("total_activity")
            .alias("avg")
        )
        .first()["avg"]
    )
 
    # --------------------------------------------------------
    # BROKEN:
    #
    # t-1, t, t+1
    #
    # t+1 = 999999, therefore the result changes.
    # --------------------------------------------------------
 
    broken_avg = (
        df
        .filter(
            (F.col("timestamp") >= datetime(2026, 1, 1, 1, 0))
            &
            (
                F.col("timestamp")
                <= t + __import__("datetime").timedelta(
                    hours=1
                )
            )
        )
        .agg(
            F.avg("total_activity")
            .alias("avg")
        )
        .first()["avg"]
    )
 
    logger.info(
        "Correct avg ending at t: %s",
        correct_avg
    )
 
    logger.info(
        "Broken avg including t+1: %s",
        broken_avg
    )
 
    # They MUST differ.
    if abs(
        correct_avg - broken_avg
    ) < 1e-9:
 
        raise AssertionError(
            "Broken implementation was NOT detected"
        )
 
    logger.info(
        "PASS: deliberately broken implementation "
        "was detected"
    )
 
 
# ============================================================
# WRITE TO MYSQL
# ============================================================
 
def write_features(features):
 
    logger.info(
        "Writing feature table to MySQL"
    )
 
    row_count = features.count()
 
    logger.info(
        "Rows to write: %d",
        row_count
    )
 
    if row_count == 0:
        raise RuntimeError(
            "Feature table contains zero rows"
        )
 
    # --------------------------------------------------------
    # Repartition before JDBC write.
    #
    # This prevents a single huge JDBC task.
    # --------------------------------------------------------
 
    output_df = (
        features
        .repartition(
            20,
            "grid_id"
        )
    )
 
    # --------------------------------------------------------
    # Write table.
    #
    # MySQL table is replaced each ML2 run.
    # --------------------------------------------------------
 
    (
        output_df
        .write
        .mode("overwrite")
        .option(
            "batchsize",
            "5000"
        )
        .option(
            "truncate",
            "true"
        )
        .jdbc(
            url=jdbc_url(),
            table=OUTPUT_TABLE,
            properties=jdbc_properties()
        )
    )
 
    logger.info(
        "Successfully wrote %d rows "
        "to %s",
        row_count,
        OUTPUT_TABLE
    )
 
 
# ============================================================
# CREATE MYSQL INDEX
# ============================================================
 
def create_index():
 
    logger.info(
        "Creating index on grid_id + feature_timestamp"
    )
 
    # Use JDBC through the Spark JVM connection.
    #
    # The pandas TEXT problem is avoided because the feature
    # table is created explicitly below if necessary.
    #
    # If Spark created grid_id as TEXT, create the index
    # using a prefix length.
    #
    # However, we first alter grid_id to VARCHAR(100).
    # --------------------------------------------------------
 
    import pymysql
 
    connection = pymysql.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=True
    )
 
    try:
 
        cursor = connection.cursor()
 
        cursor.execute(
            f"""
            ALTER TABLE {OUTPUT_TABLE}
            MODIFY COLUMN grid_id VARCHAR(100) NOT NULL
            """
        )
 
        cursor.execute(
            f"""
            ALTER TABLE {OUTPUT_TABLE}
            MODIFY COLUMN feature_timestamp DATETIME NOT NULL
            """
        )
 
        # Remove old index if it exists.
        cursor.execute(
            f"""
            DROP INDEX IF EXISTS
            idx_grid_timestamp
            ON {OUTPUT_TABLE}
            """
        )
 
        cursor.execute(
            f"""
            CREATE INDEX idx_grid_timestamp
            ON {OUTPUT_TABLE}
            (grid_id, feature_timestamp)
            """
        )
 
        logger.info(
            "Index created successfully"
        )
 
    finally:
 
        connection.close()
 
 
# ============================================================
# DATABASE VERIFICATION
# ============================================================
 
def verify_output():
 
    logger.info(
        "Verifying network_feature_table"
    )
 
    import pymysql
 
    connection = pymysql.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
 
    try:
 
        cursor = connection.cursor()
 
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {OUTPUT_TABLE}
            """
        )
 
        count = cursor.fetchone()[0]
 
        logger.info(
            "Output row count: %d",
            count
        )
 
        cursor.execute(
            f"""
            SELECT
                COUNT(*)
            FROM {OUTPUT_TABLE}
            WHERE
                avg_activity IS NULL
                OR activity_growth IS NULL
                OR active_hours IS NULL
                OR peak_ratio IS NULL
                OR variability IS NULL
                OR internet_share IS NULL
            """
        )
 
        null_count = cursor.fetchone()[0]
 
        if null_count > 0:
 
            raise AssertionError(
                f"Output contains {null_count} "
                f"rows with NULL features"
            )
 
        logger.info(
            "PASS: database output contains no NULL features"
        )
 
        cursor.execute(
            f"""
            DESCRIBE {OUTPUT_TABLE}
            """
        )
 
        logger.info(
            "Output schema:"
        )
 
        for row in cursor.fetchall():
 
            logger.info(
                "  %s | %s | %s",
                row[0],
                row[1],
                row[2]
            )
 
    finally:
 
        connection.close()
 
 
# ============================================================
# MAIN
# ============================================================
 
def main():
 
    start_time = datetime.now()
 
    logger.info(
        "=================================================="
    )
 
    logger.info(
        "ML2 — NETWORK ACTIVITY FEATURE ENGINEERING"
    )
 
    logger.info(
        "=================================================="
    )
 
    spark = None
 
    try:
 
        # ----------------------------------------------------
        # Spark
        # ----------------------------------------------------
 
        spark = create_spark_session()
 
        # ----------------------------------------------------
        # Read
        # ----------------------------------------------------
 
        source_df = read_source_data(
            spark
        )
 
        # ----------------------------------------------------
        # Clean
        # ----------------------------------------------------
 
        source_df = clean_source(
            source_df
        )
 
        # ----------------------------------------------------
        # Deduplicate
        # ----------------------------------------------------
 
        source_df = remove_duplicates(
            source_df
        )
 
        # ----------------------------------------------------
        # Cache because the data is used by:
        #
        #   feature generation
        #   hand-check
        #
        # ----------------------------------------------------
 
        source_df.cache()
 
        logger.info(
            "Source DataFrame cached"
        )
 
        # ----------------------------------------------------
        # Build features
        # ----------------------------------------------------
 
        features = build_features(
            source_df
        )
 
        # ----------------------------------------------------
        # Validate
        # ----------------------------------------------------
 
        validate_features(
            features
        )
 
        # ----------------------------------------------------
        # Hand check
        # ----------------------------------------------------
 
        hand_check(
            source_df,
            features
        )
 
        # ----------------------------------------------------
        # Leakage test
        # ----------------------------------------------------
 
        leakage_test(
            spark
        )
 
        # ----------------------------------------------------
        # Deliberately broken test
        # ----------------------------------------------------
 
        demonstrate_broken_implementation(
            spark
        )
 
        # ----------------------------------------------------
        # Write
        # ----------------------------------------------------
 
        write_features(
            features
        )
 
        # ----------------------------------------------------
        # Index
        # ----------------------------------------------------
 
        create_index()
 
        # ----------------------------------------------------
        # Verify database
        # ----------------------------------------------------
 
        verify_output()
 
        elapsed = (
            datetime.now() - start_time
        ).total_seconds()
 
        logger.info(
            "=================================================="
        )
 
        logger.info(
            "ML2 COMPLETE"
        )
 
        logger.info(
            "Execution time: %.2f seconds",
            elapsed
        )
 
        logger.info(
            "Output table: %s",
            OUTPUT_TABLE
        )
 
        logger.info(
            "Log file: %s",
            LOG_FILE
        )
 
        logger.info(
            "=================================================="
        )
 
    except Exception:
 
        logger.exception(
            "ML2 FAILED"
        )
 
        raise
 
    finally:
 
        if spark is not None:
 
            logger.info(
                "Stopping SparkSession"
            )
 
            spark.stop()
 
 
# ============================================================
# ENTRY POINT
# ============================================================
 
if __name__ == "__main__":
    main()
 