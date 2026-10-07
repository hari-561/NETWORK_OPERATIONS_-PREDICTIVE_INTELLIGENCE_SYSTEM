# ============================================================
# PHASE 2 - SP3
# Grid/Hour Consolidation and Analytics
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pathlib import Path
import logging
import os
os.environ["HADOOP_HOME"] = r"D:\capstone1\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";D:\capstone1\hadoop\bin"
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NetworkProject1")
    .master("local[2]")
    .getOrCreate()
)


# ============================================================
# 1. LOGGING SETUP
# ============================================================

log_directory = Path("D:/capstone1/log")

log_directory.mkdir(
    parents=True,
    exist_ok=True
)

log_file = log_directory / "sp3.log"


logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("SP3")


# ============================================================
# 2. START
# ============================================================

print("\n" + "=" * 60)
print("                 SP3 START")
print("=" * 60)

logger.info("=" * 60)
logger.info("SP3 START")
logger.info("=" * 60)


# ============================================================
# 3. INPUT
# ============================================================

# clean_df comes from SP2

clean_df =  spark.read.parquet(
    "D:/capstone1/parquet_output/clean_parquet"
)
input_count = clean_df.count()

print("SP2 input rows:", input_count)

logger.info(
    "SP2 input rows: %d",
    input_count
)


# ============================================================
# 4. CHECK REQUIRED COLUMNS
# ============================================================

required_columns = [
    "timestamp",
    "grid_id",
    "country_code",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity"
]

missing_columns = [
    column_name
    for column_name in required_columns
    if column_name not in clean_df.columns
]

if missing_columns:

    logger.error(
        "Missing required columns: %s",
        missing_columns
    )

    raise ValueError(
        f"Missing required columns: {missing_columns}"
    )

logger.info(
    "Required input columns verified successfully"
)


# ============================================================
# 5. COLLAPSE COUNTRY-CODE RECORDS
#
# One row per:
#       timestamp + grid_id
#
# Sum activity across country-code categories.
# ============================================================

hourly_grid_summary = (
    clean_df
    .groupBy(
        "timestamp",
        "grid_id"
    )
    .agg(
        F.sum("sms_in").alias("sms_in"),
        F.sum("sms_out").alias("sms_out"),
        F.sum("call_in").alias("call_in"),
        F.sum("call_out").alias("call_out"),
        F.sum("internet_activity").alias("internet_activity")
    )
)

logger.info(
    "Country-code records consolidated to timestamp + grid_id grain"
)


# ============================================================
# 6. VERIFY ONE ROW PER GRID + TIMESTAMP
# ============================================================

duplicate_grid_hours = (
    hourly_grid_summary
    .groupBy(
        "grid_id",
        "timestamp"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)

duplicate_count = duplicate_grid_hours.count()

print(
    "Duplicate grid + hourly timestamp records:",
    duplicate_count
)

logger.info(
    "Duplicate grid + timestamp records: %d",
    duplicate_count
)

assert duplicate_count == 0, (
    "hourly_grid_summary contains duplicate "
    "grid_id + timestamp records"
)

logger.info(
    "Grid + timestamp grain check: PASS"
)


# ============================================================
# 7. TOTAL SMS ACTIVITY
# ============================================================

hourly_grid_summary = hourly_grid_summary.withColumn(
    "total_sms",
    F.col("sms_in") + F.col("sms_out")
)


# ============================================================
# 8. TOTAL CALL ACTIVITY
# ============================================================

hourly_grid_summary = hourly_grid_summary.withColumn(
    "total_calls",
    F.col("call_in") + F.col("call_out")
)


# ============================================================
# 9. TOTAL ACTIVITY
#
# Project-defined:
# total_sms + total_calls + internet_activity
# ============================================================

hourly_grid_summary = hourly_grid_summary.withColumn(
    "total_activity",
    F.col("total_sms")
    + F.col("total_calls")
    + F.col("internet_activity")
)


# ============================================================
# 10. DERIVE DATE
# ============================================================

hourly_grid_summary = hourly_grid_summary.withColumn(
    "date",
    F.to_date("timestamp")
)


# ============================================================
# 11. DAILY TRAFFIC SUMMARY
#
# One row per grid + date
# ============================================================

daily_traffic_summary = (
    hourly_grid_summary
    .groupBy(
        "grid_id",
        "date"
    )
    .agg(
        F.sum("total_sms").alias(
            "total_sms"
        ),

        F.sum("total_calls").alias(
            "total_calls"
        ),

        F.sum("internet_activity").alias(
            "internet_activity"
        ),

        F.sum("total_activity").alias(
            "daily_activity"
        )
    )
)


daily_traffic_summary = daily_traffic_summary.withColumn(
    "internet_share",
    F.when(
        F.col("daily_activity") > 0,
        F.col("internet_activity")
        / F.col("daily_activity")
    ).otherwise(0.0)
)


daily_traffic_summary = daily_traffic_summary.cache()

daily_summary_count = daily_traffic_summary.count()

print(
    "Daily traffic summary rows:",
    daily_summary_count
)

logger.info(
    "Created daily_traffic_summary"
)

logger.info(
    "Daily traffic summary rows: %d",
    daily_summary_count
)


# ============================================================
# 12. ADD DAILY ACTIVITY BACK TO HOURLY SUMMARY
# ============================================================

hourly_grid_summary = (
    hourly_grid_summary
    .join(
        daily_traffic_summary.select(
            "grid_id",
            "date",
            "daily_activity"
        ),
        on=["grid_id", "date"],
        how="left"
    )
)


# ============================================================
# 13. INTERNET SHARE OF HOURLY TOTAL ACTIVITY
# ============================================================

hourly_grid_summary = hourly_grid_summary.withColumn(
    "internet_share",
    F.when(
        F.col("total_activity") > 0,
        F.col("internet_activity")
        / F.col("total_activity")
    ).otherwise(0.0)
)


# ============================================================
# 14. CACHE FINAL HOURLY GRID SUMMARY
# ============================================================

hourly_grid_summary = hourly_grid_summary.cache()

hourly_grid_count = hourly_grid_summary.count()

print(
    "Hourly grid summary rows:",
    hourly_grid_count
)

logger.info(
    "Created hourly_grid_summary"
)

logger.info(
    "Hourly grid summary rows: %d",
    hourly_grid_count
)


# ============================================================
# 15. FINAL GRAIN CHECK
#
# Exactly one record per:
#       grid_id + hourly timestamp
# ============================================================

final_duplicate_count = (
    hourly_grid_summary
    .groupBy(
        "grid_id",
        "timestamp"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    "Final duplicate grid-hour records:",
    final_duplicate_count
)

logger.info(
    "Final duplicate grid-hour records: %d",
    final_duplicate_count
)

assert final_duplicate_count == 0, (
    "Final hourly_grid_summary is not "
    "at the required grain"
)

logger.info(
    "Final hourly_grid_summary grain check: PASS"
)


# ============================================================
# 16. TOP TEN HIGH-ACTIVITY GRIDS
# ============================================================

def top_ten_grids(
    dataframe,
    start_date=None,
    end_date=None
):
    """
    Return the top 10 grids by total activity
    for the selected date window.
    """

    window_df = dataframe

    if start_date is not None:

        window_df = window_df.filter(
            F.col("date") >= F.lit(start_date)
        )

    if end_date is not None:

        window_df = window_df.filter(
            F.col("date") <= F.lit(end_date)
        )

    return (
        window_df
        .groupBy("grid_id")
        .agg(
            F.sum(
                "total_activity"
            ).alias(
                "window_total_activity"
            )
        )
        .orderBy(
            F.desc("window_total_activity")
        )
        .limit(10)
    )


# ============================================================
# 17. SELECTED WINDOW
# ============================================================

# Set these according to the project requirement.
# None means use the complete available dataset.

selected_start_date = None
selected_end_date = None


# ============================================================
# 18. HOTSPOT RANKING
# ============================================================

hotspot_ranking = top_ten_grids(
    hourly_grid_summary,
    start_date=selected_start_date,
    end_date=selected_end_date
)


print(
    "\n========== HOTSPOT RANKING =========="
)

hotspot_ranking.show(
    10,
    truncate=False
)


logger.info(
    "Created hotspot ranking"
)

logger.info(
    "Hotspot ranking contains top 10 high-activity grids"
)

if selected_start_date is not None:
    logger.info(
        "Hotspot start date: %s",
        selected_start_date
    )

if selected_end_date is not None:
    logger.info(
        "Hotspot end date: %s",
        selected_end_date
    )


# ============================================================
# 19. PEAK ACTIVITY HOUR PER GRID
# ============================================================

peak_window = (
    Window
    .partitionBy("grid_id")
    .orderBy(
        F.desc("total_activity"),
        F.asc("timestamp")
    )
)


peak_activity_df = (
    hourly_grid_summary
    .withColumn(
        "peak_rank",
        F.row_number().over(peak_window)
    )
    .filter(
        F.col("peak_rank") == 1
    )
    .select(
        "grid_id",
        "timestamp",
        "total_activity"
    )
    .withColumnRenamed(
        "timestamp",
        "peak_activity_hour"
    )
    .withColumnRenamed(
        "total_activity",
        "peak_activity"
    )
)


print(
    "\n========== PEAK ACTIVITY HOUR PER GRID =========="
)

peak_activity_df.show(
    20,
    truncate=False
)


logger.info(
    "Computed peak activity hour for each grid"
)


# ============================================================
# 20. OVERALL PEAK ACTIVITY HOUR
# ============================================================

overall_peak = (
    hourly_grid_summary
    .orderBy(
        F.desc("total_activity"),
        F.asc("timestamp")
    )
    .select(
        "grid_id",
        "timestamp",
        "total_activity"
    )
    .first()
)


if overall_peak is not None:

    overall_peak_result = {
        "grid_id": str(
            overall_peak["grid_id"]
        ),
        "timestamp": str(
            overall_peak["timestamp"]
        ),
        "total_activity": float(
            overall_peak["total_activity"]
        )
    }

else:

    overall_peak_result = None


print(
    "\n========== OVERALL PEAK ACTIVITY =========="
)

print(
    overall_peak_result
)


logger.info(
    "Overall peak activity: %s",
    overall_peak_result
)


# ============================================================
# 21. UNIQUE GRIDS
# ============================================================

unique_grid_count = (
    hourly_grid_summary
    .select("grid_id")
    .distinct()
    .count()
)


logger.info(
    "Unique grids in hourly_grid_summary: %d",
    unique_grid_count
)


# ============================================================
# 22. UNIQUE HOURLY TIMESTAMPS
# ============================================================

unique_hour_count = (
    hourly_grid_summary
    .select("timestamp")
    .distinct()
    .count()
)


logger.info(
    "Unique hourly timestamps: %d",
    unique_hour_count
)


# ============================================================
# 23. NEGATIVE ACTIVITY VALIDATION
# ============================================================

negative_activity_count = (
    hourly_grid_summary
    .filter(
        (F.col("total_sms") < 0)
        | (F.col("total_calls") < 0)
        | (F.col("internet_activity") < 0)
        | (F.col("total_activity") < 0)
    )
    .count()
)


print(
    "Negative aggregated activity rows:",
    negative_activity_count
)


logger.info(
    "Negative aggregated activity rows: %d",
    negative_activity_count
)


assert negative_activity_count == 0, (
    "Negative activity detected in "
    "hourly_grid_summary"
)

logger.info(
    "Negative activity check: PASS"
)


# ============================================================
# 24. INSPECT HOURLY GRID SUMMARY
# ============================================================

print(
    "\n========== HOURLY GRID SUMMARY SCHEMA =========="
)

hourly_grid_summary.printSchema()


print(
    "\n========== HOURLY GRID SUMMARY SAMPLE =========="
)

hourly_grid_summary.orderBy(
    "grid_id",
    "timestamp"
).show(
    10,
    truncate=False
)


# ============================================================
# 25. INSPECT DAILY TRAFFIC SUMMARY
# ============================================================

print(
    "\n========== DAILY TRAFFIC SUMMARY =========="
)

daily_traffic_summary.orderBy(
    "grid_id",
    "date"
).show(
    10,
    truncate=False
)


# ============================================================
# 26. FINAL SP3 VALIDATION
# ============================================================

grain_check = (
    final_duplicate_count == 0
)

negative_check = (
    negative_activity_count == 0
)


if grain_check and negative_check:

    sp3_status = "PASS"

else:

    sp3_status = "FAIL"


logger.info(
    "SP3 grain check: %s",
    "PASS" if grain_check else "FAIL"
)

logger.info(
    "SP3 negative activity check: %s",
    "PASS" if negative_check else "FAIL"
)

logger.info(
    "SP3 status: %s",
    sp3_status
)


# ============================================================
# 27. FINAL LOG SUMMARY
# ============================================================

logger.info("=" * 60)
logger.info("SP3 FINAL SUMMARY")
logger.info("=" * 60)

logger.info(
    "SP2 input rows: %d",
    input_count
)

logger.info(
    "hourly_grid_summary rows: %d",
    hourly_grid_count
)

logger.info(
    "daily_traffic_summary rows: %d",
    daily_summary_count
)

logger.info(
    "Unique grids: %d",
    unique_grid_count
)

logger.info(
    "Unique hourly timestamps: %d",
    unique_hour_count
)

logger.info(
    "Duplicate grid-hour records: %d",
    final_duplicate_count
)

logger.info(
    "Negative activity records: %d",
    negative_activity_count
)

logger.info(
    "Expected output 1: hourly_grid_summary - CREATED"
)

logger.info(
    "Expected output 2: daily_traffic_summary - CREATED"
)

logger.info(
    "Expected output 3: hotspot ranking - CREATED"
)

logger.info(
    "SP3 completed with status: %s",
    sp3_status
)

logger.info("=" * 60)


# ============================================================
# 28. CONSOLE SUMMARY
# ============================================================

print("\n" + "=" * 60)
print("                 SP3 SUMMARY")
print("=" * 60)

print(
    f"SP2 input rows           : {input_count}"
)

print(
    f"hourly_grid_summary rows : {hourly_grid_count}"
)

print(
    f"daily_traffic_summary    : {daily_summary_count}"
)

print(
    f"Unique grids             : {unique_grid_count}"
)

print(
    f"Unique hourly timestamps : {unique_hour_count}"
)

print(
    f"Duplicate grid-hours     : {final_duplicate_count}"
)

print(
    f"Negative activity rows   : {negative_activity_count}"
)

print(
    f"Grain check              : "
    f"{'PASS' if grain_check else 'FAIL'}"
)

print(
    f"Negative activity check  : "
    f"{'PASS' if negative_check else 'FAIL'}"
)

print(
    f"SP3 status               : {sp3_status}"
)

print("=" * 60)

print(
    f"\nSP3 log saved to: {log_file}"
)


# ============================================================
# SAVE SP3 OUTPUTS
# ============================================================



# ------------------------------------------------------------
# 1. hourly_grid_summary.csv
# ------------------------------------------------------------

hourly_grid_summary.write.mode("overwrite").parquet(
    "D:/capstone1/parquet_output/hourly_grid_parquet")

logger.info(
    "hourly_grid_summary written successfully"
)


# ------------------------------------------------------------
# 2. daily_traffic_summary.csv
# ------------------------------------------------------------

daily_traffic_summary.write.mode("overwrite").parquet(
    "D:/capstone1/parquet_output/daily_traffic_parquet")



logger.info(
    "daily_traffic_summary written successfully"
)


# ------------------------------------------------------------
# 3. hotspot ranking
# ------------------------------------------------------------

# hotspot_ranking.write \
#     .mode("overwrite") \
#     .option("header", True) \
#     .csv(
#         str(output_directory / "hotspot_ranking")
#     )

# logger.info(
#     "hotspot_ranking written successfully"
# )