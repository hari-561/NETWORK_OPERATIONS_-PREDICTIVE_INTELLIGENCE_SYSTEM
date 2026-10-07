"""
Standalone ML2 Leakage Test
 
Tests whether adding future data at t+1 changes
the features calculated at time t.
 
This file does NOT read, modify, or overwrite
the existing ML2 table.
"""
 
import logging
from datetime import datetime, timedelta
 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType
)
from pyspark.sql.window import Window
 
 
# ============================================================
# LOGGING
# ============================================================
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
 
logger = logging.getLogger("ML2_LEAKAGE_TEST")
 
 
# ============================================================
# SAME SETTINGS AS ML2
# ============================================================
 
RECENT_WINDOW_HOURS = 2
BASELINE_WINDOW_HOURS = 1
 
 
# ============================================================
# BUILD FEATURES
# ============================================================
 
def build_features(df):
 
    feature_window = (
        Window
        .partitionBy("grid_id")
        .orderBy("timestamp")
        .rowsBetween(
            -(RECENT_WINDOW_HOURS - 1),
            0
        )
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
 
    df = (
        df
        .withColumn(
            "avg_activity",
            F.avg("total_activity").over(
                feature_window
            )
        )
        .withColumn(
            "_peak_activity",
            F.max("total_activity").over(
                feature_window
            )
        )
        .withColumn(
            "active_hours",
            F.sum(
                F.when(
                    F.col("total_activity") > 0,
                    1
                ).otherwise(0)
            ).over(feature_window)
        )
        .withColumn(
            "variability",
            F.stddev_pop(
                "total_activity"
            ).over(feature_window)
        )
        .withColumn(
            "_recent_total_activity",
            F.sum(
                "total_activity"
            ).over(feature_window)
        )
        .withColumn(
            "_recent_internet_activity",
            F.sum(
                "internet_activity"
            ).over(feature_window)
        )
        .withColumn(
            "_baseline_avg",
            F.avg(
                "total_activity"
            ).over(baseline_window)
        )
    )
 
    df = (
        df
        .withColumn(
            "activity_growth",
            F.when(
                F.col("_baseline_avg").isNull()
                | (F.col("_baseline_avg") == 0),
                F.lit(0.0)
            ).otherwise(
                (
                    F.col("avg_activity")
                    - F.col("_baseline_avg")
                )
                / F.col("_baseline_avg")
            )
        )
        .withColumn(
            "peak_ratio",
            F.when(
                F.col("avg_activity").isNull()
                | (F.col("avg_activity") == 0),
                F.lit(0.0)
            ).otherwise(
                F.col("_peak_activity")
                / F.col("avg_activity")
            )
        )
        .withColumn(
            "internet_share",
            F.when(
                F.col("_recent_total_activity") == 0,
                F.lit(0.0)
            ).otherwise(
                F.col("_recent_internet_activity")
                / F.col("_recent_total_activity")
            )
        )
        .withColumn(
            "variability",
            F.coalesce(
                F.col("variability"),
                F.lit(0.0)
            )
        )
    )
 
    # --------------------------------------------------------
    # Require 48 hours of history
    # --------------------------------------------------------
 
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
 
    df = df.filter(
        F.col("_history_count") >= 48
    )
 
    df = df.withColumn(
        "feature_timestamp",
        F.col("timestamp")
    )
 
    return df.select(
        "grid_id",
        "feature_timestamp",
        "avg_activity",
        "activity_growth",
        "active_hours",
        "peak_ratio",
        "variability",
        "internet_share"
    )
 
 
# ============================================================
# LEAKAGE TEST
# ============================================================
 
def leakage_test(spark):
 
    logger.info("==========================================")
    logger.info("STARTING ML2 LEAKAGE TEST")
    logger.info("==========================================")
 
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
 
    # --------------------------------------------------------
    # IMPORTANT
    #
    # We need:
    #
    # 48 hours BEFORE t
    # + t
    #
    # Therefore data_before contains 49 rows.
    #
    # t     = 2026-01-03 00:00
    # t+1   = 2026-01-03 01:00
    # --------------------------------------------------------
 
    start_time = datetime(
        2026,
        1,
        1,
        0,
        0
    )
 
    t = datetime(
        2026,
        1,
        3,
        0,
        0
    )
 
    t_plus_1 = datetime(
        2026,
        1,
        3,
        1,
        0
    )
 
    # --------------------------------------------------------
    # Create 49 rows:
    #
    # Jan 1 00:00
    # ...
    # Jan 3 00:00 = t
    # --------------------------------------------------------
 
    data_before = []
 
    for i in range(49):
 
        timestamp = (
            start_time
            + timedelta(hours=i)
        )
 
        data_before.append(
            (
                "TEST_GRID",
                timestamp,
                10.0 + i,
                100.0 + (i * 10)
            )
        )
 
    # --------------------------------------------------------
    # Add EXTREME future value at t+1
    #
    # If any feature incorrectly uses future data,
    # this extreme value should change the result.
    # --------------------------------------------------------
 
    future_row = (
        "TEST_GRID",
        t_plus_1,
        9999.0,
        999999.0
    )
 
    data_after = (
        data_before
        + [future_row]
    )
 
    logger.info(
        f"Rows before future data: {len(data_before)}"
    )
 
    logger.info(
        f"Rows after future data: {len(data_after)}"
    )
 
    logger.info(
        f"Target t: {t}"
    )
 
    logger.info(
        f"Future t+1: {t_plus_1}"
    )
 
    # --------------------------------------------------------
    # Create DataFrames
    # --------------------------------------------------------
 
    df_before = spark.createDataFrame(
        data_before,
        schema
    )
 
    df_after = spark.createDataFrame(
        data_after,
        schema
    )
 
    # --------------------------------------------------------
    # Build features
    # --------------------------------------------------------
 
    logger.info(
        "Building features BEFORE adding t+1"
    )
 
    features_before = build_features(
        df_before
    )
 
    logger.info(
        "Building features AFTER adding t+1"
    )
 
    features_after = build_features(
        df_after
    )
 
    # --------------------------------------------------------
    # Get target row
    # --------------------------------------------------------
 
    row_before = (
        features_before
        .filter(
            F.col("feature_timestamp") == F.lit(t)
        )
        .collect()
    )
 
    row_after = (
        features_after
        .filter(
            F.col("feature_timestamp") == F.lit(t)
        )
        .collect()
    )
 
    # --------------------------------------------------------
    # Check row exists
    # --------------------------------------------------------
 
    if len(row_before) == 0:
 
        logger.error(
            "Target t does not exist in features_before"
        )
 
        features_before.select(
            "feature_timestamp"
        ).orderBy(
            "feature_timestamp"
        ).show(
            truncate=False
        )
 
        raise AssertionError(
            "Leakage test could not find t row"
        )
 
    if len(row_after) == 0:
 
        logger.error(
            "Target t does not exist in features_after"
        )
 
        features_after.select(
            "feature_timestamp"
        ).orderBy(
            "feature_timestamp"
        ).show(
            truncate=False
        )
 
        raise AssertionError(
            "Leakage test could not find t row "
            "after adding t+1"
        )
 
    row_before = row_before[0]
    row_after = row_after[0]
 
    # --------------------------------------------------------
    # Features to test
    # --------------------------------------------------------
 
    feature_columns = [
        "avg_activity",
        "activity_growth",
        "active_hours",
        "peak_ratio",
        "variability",
        "internet_share"
    ]
 
    logger.info("------------------------------------------")
    logger.info("COMPARING FEATURES AT t")
    logger.info("------------------------------------------")
 
    # --------------------------------------------------------
    # Compare each feature
    # --------------------------------------------------------
 
    for column in feature_columns:
 
        before_value = row_before[column]
        after_value = row_after[column]
 
        if before_value is None:
            before_value = 0.0
 
        if after_value is None:
            after_value = 0.0
 
        difference = abs(
            float(before_value)
            - float(after_value)
        )
 
        logger.info(
            f"{column}: "
            f"before={before_value}, "
            f"after={after_value}, "
            f"difference={difference}"
        )
 
        if difference >= 1e-9:
 
            raise AssertionError(
                f"LEAKAGE DETECTED: {column} "
                f"changed after adding future t+1 data"
            )
 
    # --------------------------------------------------------
    # SUCCESS
    # --------------------------------------------------------
 
    logger.info("------------------------------------------")
    logger.info(
        "PASS: Future t+1 data does NOT change "
        "features at t"
    )
    logger.info(
        "ML2 LEAKAGE TEST PASSED"
    )
    logger.info("==========================================")
 
 
# ============================================================
# MAIN
# ============================================================
 
def main():
 
    spark = None
 
    try:
 
        logger.info(
            "Starting isolated ML2 leakage test"
        )
 
        spark = (
            SparkSession
            .builder
            .appName("ML2_Leakage_Test")
            .master("local[2]")
            .config(
                "spark.sql.shuffle.partitions",
                "2"
            )
            .config(
                "spark.python.worker.reuse",
                "true"
            )
            .getOrCreate()
        )
 
        spark.sparkContext.setLogLevel(
            "WARN"
        )
 
        leakage_test(spark)
 
    except Exception:
 
        logger.exception(
            "ML2 LEAKAGE TEST FAILED"
        )
 
        raise
 
    finally:
 
        if spark is not None:
 
            logger.info(
                "Stopping SparkSession"
            )
 
            spark.stop()
 
 
if __name__ == "__main__":
    main()
 