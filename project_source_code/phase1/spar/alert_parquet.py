import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";D:\hadoop\bin"
# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("NP3")


# ============================================================
# Paths
# ============================================================

INPUT_PATH = (
    "../../data/processed/hourly_grid_summary_parquet"
)

ALERT_OUTPUT_PATH = (
    "../../data/processed/activity_alerts_parquet"
)

HOTSPOT_OUTPUT_PATH = (
    "../../data/processed/hotspot_parquet"
)


# ============================================================
# Configuration
# ============================================================

HIGH_MULTIPLIER = 1.50
SPIKE_MULTIPLIER = 1.50
DROP_MULTIPLIER = 0.50

HOTSPOT_TOP_N = 10


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("NP3-Alert-Detection")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 1. Read hourly grid summary
# ============================================================

logger.info(
    "Reading hourly grid summary from: %s",
    INPUT_PATH
)

df = spark.read.parquet(INPUT_PATH)

logger.info(
    "Input rows: %d",
    df.count()
)


# ============================================================
# 2. Validate required columns
# ============================================================

required_columns = [
    "timestamp",
    "grid_id",
    "total_activity"
]

missing_columns = [
    column
    for column in required_columns
    if column not in df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing required columns: {missing_columns}"
    )


# ============================================================
# 3. Clean / validate
# ============================================================

df = (
    df
    .withColumn(
        "timestamp",
        F.to_timestamp("timestamp")
    )
    .withColumn(
        "total_activity",
        F.col("total_activity").cast("double")
    )
)

invalid_count = (
    df
    .filter(
        F.col("timestamp").isNull()
        | F.col("grid_id").isNull()
        | F.col("total_activity").isNull()
        | (F.col("total_activity") < 0)
    )
    .count()
)

if invalid_count > 0:
    logger.warning(
        "Removing %d invalid rows",
        invalid_count
    )

df = (
    df
    .filter(
        F.col("timestamp").isNotNull()
        & F.col("grid_id").isNotNull()
        & F.col("total_activity").isNotNull()
        & (F.col("total_activity") >= 0)
    )
)


# ============================================================
# 4. Derive date / hour
# ============================================================

df = (
    df
    .withColumn("date", F.to_date("timestamp"))
    .withColumn("hour", F.hour("timestamp"))
)


# ============================================================
# 5. Daily grid activity
# ============================================================

logger.info("Calculating daily grid activity")

daily_activity = (
    df
    .groupBy("grid_id", "date")
    .agg(
        F.sum("total_activity")
        .alias("daily_total_activity")
    )
)


# ============================================================
# 6. Activity floor
# ============================================================

logger.info(
    "Calculating activity floor using 10th percentile"
)

activity_floor = (
    daily_activity
    .approxQuantile(
        "daily_total_activity",
        [0.10],
        0.01
    )[0]
)

logger.info(
    "Activity floor: %.4f",
    activity_floor
)


# ============================================================
# 7. Join daily activity back
# ============================================================

df = (
    df
    .join(
        daily_activity,
        on=["grid_id", "date"],
        how="left"
    )
)


# ============================================================
# 8. Daily coverage
# ============================================================

coverage = (
    df
    .groupBy("grid_id", "date")
    .agg(
        F.countDistinct("hour")
        .alias("hour_count")
    )
)

df = (
    df
    .join(
        coverage,
        on=["grid_id", "date"],
        how="left"
    )
    .withColumn(
        "complete_day",
        F.col("hour_count") == 24
    )
)

# ============================================================
# 9. Leave-one-out median baseline
# ============================================================
logger.info(
    "Calculating leave-one-out median baseline"
)

group_window = (
    Window
    .partitionBy("grid_id", "date")
    .orderBy("timestamp")
)

df = df.withColumn(
    "_position",
    F.row_number().over(group_window)
)

baseline_window = (
    Window
    .partitionBy("grid_id", "date")
    .orderBy("_position")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
    )
)

df = df.withColumn(
    "_all_values",
    F.collect_list("total_activity").over(
        baseline_window
    )
)

df = df.withColumn(
    "_before",
    F.expr(
        "slice(_all_values, 1, _position - 1)"
    )
)

df = df.withColumn(
    "_after",
    F.expr(
        """
        slice(
            _all_values,
            _position + 1,
            size(_all_values)
        )
        """
    )
)

df = df.withColumn(
    "_other_values",
    F.concat(
        F.col("_before"),
        F.col("_after")
    )
)

df = df.withColumn(
    "_sorted_other_values",
    F.array_sort(
        F.col("_other_values")
    )
)

df = df.withColumn(
    "baseline_activity",
    F.when(
        F.size("_sorted_other_values") == 23,
        F.element_at(
            "_sorted_other_values",
            12
        )
    )
)

# ============================================================
# 10. Previous-hour activity
# ============================================================

previous_window = (
    Window
    .partitionBy("grid_id")
    .orderBy("timestamp")
)

df = df.withColumn(
    "previous_activity",
    F.lag("total_activity").over(
        previous_window
    )
)


# ============================================================
# 11. Alert eligibility
# ============================================================

df = df.withColumn(
    "eligible_for_alert",
    (
        (F.col("daily_total_activity") >= activity_floor)
        &
        F.col("complete_day")
    )
)


# ============================================================
# 12. Apply rules
# ============================================================

df = (
    df
    .withColumn(
        "high_activity",
        (
            F.col("eligible_for_alert")
            &
            (
                F.col("total_activity")
                >=
                HIGH_MULTIPLIER
                * F.col("baseline_activity")
            )
        )
    )
    .withColumn(
        "activity_drop",
        (
            F.col("eligible_for_alert")
            &
            (
                F.col("total_activity")
                <=
                DROP_MULTIPLIER
                * F.col("baseline_activity")
            )
        )
    )
    .withColumn(
        "activity_spike",
        (
            F.col("eligible_for_alert")
            &
            F.col("previous_activity").isNotNull()
            &
            (
                F.col("total_activity")
                >=
                SPIKE_MULTIPLIER
                * F.col("previous_activity")
            )
        )
    )
)


# ============================================================
# 13. Create alert records
# ============================================================

logger.info("Creating alert records")

high_alerts = (
    df
    .filter(F.col("high_activity"))
    .select(
        "grid_id",
        "timestamp",
        F.lit("HIGH_ACTIVITY").alias("alert_type"),
        F.col("total_activity")
        .alias("current_activity"),
        "baseline_activity"
    )
    .withColumn(
        "reason",
        F.concat(
            F.lit("Current activity "),
            F.round("current_activity", 2),
            F.lit(" is at least "),
            F.lit(HIGH_MULTIPLIER),
            F.lit("x the within-day baseline.")
        )
    )
)


spike_alerts = (
    df
    .filter(F.col("activity_spike"))
    .select(
        "grid_id",
        "timestamp",
        F.lit("ACTIVITY_SPIKE").alias("alert_type"),
        F.col("total_activity")
        .alias("current_activity"),
        "baseline_activity",
        "previous_activity"
    )
    .withColumn(
        "reason",
        F.concat(
            F.lit("Current activity "),
            F.round("current_activity", 2),
            F.lit(" is at least "),
            F.lit(SPIKE_MULTIPLIER),
            F.lit("x the preceding hour.")
        )
    )
    .drop("previous_activity")
)


drop_alerts = (
    df
    .filter(F.col("activity_drop"))
    .select(
        "grid_id",
        "timestamp",
        F.lit("ACTIVITY_DROP").alias("alert_type"),
        F.col("total_activity")
        .alias("current_activity"),
        "baseline_activity"
    )
    .withColumn(
        "reason",
        F.concat(
            F.lit("Current activity "),
            F.round("current_activity", 2),
            F.lit(" is at most "),
            F.lit(DROP_MULTIPLIER),
            F.lit("x the within-day baseline.")
        )
    )
)


alerts = (
    high_alerts
    .unionByName(spike_alerts)
    .unionByName(drop_alerts)
)


# ============================================================
# 14. Write alerts
# ============================================================

logger.info(
    "Writing alerts to %s",
    ALERT_OUTPUT_PATH
)

(
    alerts
    .repartition("grid_id")
    .write
    .mode("overwrite")
    .partitionBy("grid_id")
    .parquet(ALERT_OUTPUT_PATH)
)


# ============================================================
# 15. Hotspot calculation
# ============================================================

logger.info("Calculating hotspots")

hotspot_window = (
    Window
    .partitionBy("timestamp")
    .orderBy(
        F.col("total_activity").desc()
    )
)

hotspots = (
    df
    .withColumn(
        "hotspot_rank",
        F.row_number().over(
            Window
            .partitionBy("timestamp")
            .orderBy(F.col("total_activity").desc())
        )
    )
    .select(
        "timestamp",
        "date",
        "grid_id",
        "total_activity",
        "hotspot_rank"
    )
)

# ============================================================
# 16. Write hotspots
# ============================================================

logger.info(
    "Writing hotspots to %s",
    HOTSPOT_OUTPUT_PATH
)

(
    hotspots
    .repartition("date")
    .write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(HOTSPOT_OUTPUT_PATH)
)


# ============================================================
# 17. Operational summary
# ============================================================

total_alerts = alerts.count()

logger.info(
    "=============================================="
)

logger.info(
    "NP3 OPERATIONAL SUMMARY"
)

logger.info(
    "Activity floor: %.4f",
    activity_floor
)

logger.info(
    "Total alert records: %d",
    total_alerts
)

logger.info(
    "HIGH_ACTIVITY: %d",
    high_alerts.count()
)

logger.info(
    "ACTIVITY_SPIKE: %d",
    spike_alerts.count()
)

logger.info(
    "ACTIVITY_DROP: %d",
    drop_alerts.count()
)

logger.info(
    "NP3 completed successfully"
)

spark.stop()
