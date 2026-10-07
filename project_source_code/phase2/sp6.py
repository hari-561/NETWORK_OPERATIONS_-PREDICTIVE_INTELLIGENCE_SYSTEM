# ============================================================
# PHASE 2 - SP6
# Storage and Parquet Output
# ============================================================

import logging
import shutil
from pathlib import Path

from pyspark.sql import functions as F

import os
os.environ["HADOOP_HOME"] = r"D:\capstone1\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";D:\capstone1\hadoop\bin"
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NetworkProject1")
    .master("local[*]")
    .getOrCreate()
)

# ============================================================
# 1. LOGGING SETUP
# ============================================================

log_directory = Path("../log")

log_directory.mkdir(
    parents=True,
    exist_ok=True
)

log_file = log_directory / "sp6.log"


logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("SP6")


logger.info("=" * 60)
logger.info("SP6 START")
logger.info("=" * 60)


print("\n" + "=" * 60)
print("                 SP6 START")
print("=" * 60)


# ============================================================
# 2. REQUIRED INPUT DATAFRAMES
# ============================================================
#
# From previous stages:
#
# cleaned_activity_df
# hourly_grid_summary
#
# If your SP2 DataFrame has a different name, change it here.
# ============================================================
cleaned_activity_df = spark.read.parquet(
    "../parquet_output/clean_parquet"
)

hourly_grid_summary = spark.read.parquet(
    "../parquet_output/hourly_grid_parquet"
)



logger.info(
    "Required input DataFrames are available"
)


# ============================================================
# 3. OUTPUT DIRECTORIES
# ============================================================

base_data_directory = Path("../data")

curated_directory = (
    base_data_directory / "curated"
)

reference_directory = (
    base_data_directory / "reference"
)

dashboard_directory = (
    base_data_directory / "dashboard"
)

curated_directory.mkdir(
    parents=True,
    exist_ok=True
)

reference_directory.mkdir(
    parents=True,
    exist_ok=True
)

dashboard_directory.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# 4. OUTPUT PATHS
# ============================================================

clean_activity_parquet_path = (
    curated_directory
    / "clean_activity_parquet"
)

hourly_grid_parquet_path = (
    curated_directory
    / "hourly_grid_summary_parquet"
)

dashboard_csv_path = (
    dashboard_directory
    / "dashboard_summary"
)

reference_geojson_path = (
    reference_directory
    / "milano-grid.geojson"
)


# ============================================================
# 5. BASIC INPUT COUNTS
# ============================================================

clean_activity_count = (
    cleaned_activity_df.count()
)

hourly_grid_count = (
    hourly_grid_summary.count()
)


print("\n========== INPUT COUNTS ==========")

print(
    "Clean activity rows:",
    clean_activity_count
)

print(
    "Hourly grid summary rows:",
    hourly_grid_count
)


logger.info(
    "Clean activity input rows: %d",
    clean_activity_count
)

logger.info(
    "Hourly grid summary input rows: %d",
    hourly_grid_count
)


# ============================================================
# 6. VERIFY DATE COLUMN
# ============================================================
#
# SP2 should already have derived:
#
# date
# hour
# day_of_week
#
# The clean activity output must have date available for
# partitioning.
# ============================================================

if "date" not in cleaned_activity_df.columns:

    logger.error(
        "date column is missing from cleaned activity DataFrame"
    )

    raise ValueError(
        "SP2 cleaned activity DataFrame must contain "
        "a 'date' column."
    )


logger.info(
    "Date column available for partitioning: PASS"
)


# ============================================================
# 7. PREPARE CLEAN ACTIVITY OUTPUT
# ============================================================
#
# We preserve the cleaned activity data.
#
# Do NOT add geometry here.
#
# Geometry belongs to the static reference dataset.
# ============================================================

clean_activity_output = (
    cleaned_activity_df
)


# ============================================================
# 8. WRITE CLEAN ACTIVITY AS PARQUET
#
# Partition by date.
# ============================================================

print(
    "\n========== WRITE CLEAN ACTIVITY PARQUET =========="
)


(
    clean_activity_output
    .write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(
        str(clean_activity_parquet_path)
    )
)


logger.info(
    "Clean activity written as Parquet: %s",
    clean_activity_parquet_path
)

logger.info(
    "Clean activity Parquet partitioned by date"
)


print(
    "Clean activity Parquet written successfully."
)


# ============================================================
# 9. PREPARE HOURLY GRID SUMMARY
#
# Requirement:
#
# One record per grid_id + hourly timestamp.
#
# IMPORTANT:
# Geometry is deliberately NOT included.
# ============================================================

print(
    "\n========== PREPARE HOURLY GRID SUMMARY =========="
)


# ------------------------------------------------------------
# Required analytical columns
# ------------------------------------------------------------

required_hourly_columns = [
    "timestamp",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_activity"
]


missing_hourly_columns = [
    column
    for column in required_hourly_columns
    if column not in hourly_grid_summary.columns
]


if missing_hourly_columns:

    logger.error(
        "Missing hourly summary columns: %s",
        missing_hourly_columns
    )

    raise ValueError(
        "hourly_grid_summary is missing columns: "
        + str(missing_hourly_columns)
    )


hourly_grid_output = (
    hourly_grid_summary
    .select(
        *required_hourly_columns
    )
)


# ============================================================
# 10. VALIDATE ONE RECORD PER GRID + HOUR
# ============================================================

hourly_duplicate_groups = (
    hourly_grid_output
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
    "Duplicate grid + timestamp groups:",
    hourly_duplicate_groups
)


logger.info(
    "Duplicate grid + timestamp groups: %d",
    hourly_duplicate_groups
)


assert hourly_duplicate_groups == 0, (
    "hourly_grid_summary contains duplicate "
    "grid_id + timestamp records"
)


logger.info(
    "One record per grid + hour validation: PASS"
)


# ============================================================
# 11. VERIFY GEOMETRY IS NOT IN ANALYTICS OUTPUT
# ============================================================

if "geometry" in hourly_grid_output.columns:

    logger.error(
        "Geometry is present in hourly_grid_summary output"
    )

    raise ValueError(
        "Geometry must remain in the static grid reference "
        "and must not be duplicated in hourly analytics."
    )


logger.info(
    "Geometry separation validation: PASS"
)


# ============================================================
# 12. WRITE HOURLY GRID SUMMARY AS PARQUET
# ============================================================

print(
    "\n========== WRITE HOURLY GRID PARQUET =========="
)


(
    hourly_grid_output
    .write
    .mode("overwrite")
    .parquet(
        str(hourly_grid_parquet_path)
    )
)


logger.info(
    "hourly_grid_summary written as Parquet: %s",
    hourly_grid_parquet_path
)


print(
    "hourly_grid_summary Parquet written successfully."
)


# ============================================================
# 13. COPY GEOJSON TO REFERENCE DIRECTORY
# ============================================================

print(
    "\n========== STATIC GRID REFERENCE =========="
)


# ------------------------------------------------------------
# Find the GeoJSON.
#
# Change this source path if your original SP4 file is stored
# elsewhere.
# ------------------------------------------------------------

source_geojson_candidates = [
    Path("../data/raw/milano-grid.geojson"),
    Path("milano-grid.geojson"),
    Path("data/milano-grid.geojson")
]


source_geojson_path = None


for candidate in source_geojson_candidates:

    if candidate.exists():

        source_geojson_path = candidate
        break


if source_geojson_path is None:

    logger.error(
        "milano-grid.geojson could not be found"
    )

    raise FileNotFoundError(
        "Could not find milano-grid.geojson"
    )


shutil.copy2(
    source_geojson_path,
    reference_geojson_path
)


logger.info(
    "Static GeoJSON retained separately at: %s",
    reference_geojson_path
)


print(
    "Static GeoJSON reference:",
    reference_geojson_path
)


# ============================================================
# 14. CREATE SMALL DASHBOARD SUMMARY
# ============================================================
#
# This is intentionally a SMALL CSV for easy inspection.
#
# It is not the full analytics dataset.
# ============================================================


print(
    "\n========== DASHBOARD SUMMARY =========="
)


dashboard_summary = (
    hourly_grid_output
    .groupBy("timestamp")
    .agg(
        F.sum(
            "total_activity"
        ).alias(
            "total_activity"
        ),

        F.sum(
            "sms_in"
        ).alias(
            "total_sms_in"
        ),

        F.sum(
            "sms_out"
        ).alias(
            "total_sms_out"
        ),

        F.sum(
            "call_in"
        ).alias(
            "total_call_in"
        ),

        F.sum(
            "call_out"
        ).alias(
            "total_call_out"
        ),

        F.sum(
            "internet_activity"
        ).alias(
            "total_internet_activity"
        ),

        F.countDistinct(
            "grid_id"
        ).alias(
            "active_grid_count"
        )
    )
    .orderBy(
        "timestamp"
    )
)


dashboard_row_count = (
    dashboard_summary.count()
)


print(
    "Dashboard summary rows:",
    dashboard_row_count
)


logger.info(
    "Dashboard summary rows: %d",
    dashboard_row_count
)


# ============================================================
# 15. WRITE DASHBOARD CSV
# ============================================================
#
# Since this is a small inspection dataset, coalesce(1)
# is reasonable.
# ============================================================

(
    dashboard_summary
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(
        str(dashboard_csv_path)
    )
)


logger.info(
    "Dashboard summary written as CSV: %s",
    dashboard_csv_path
)


print(
    "Dashboard summary CSV written successfully."
)


# ============================================================
# 16. READ CLEAN ACTIVITY PARQUET BACK
# ============================================================

print(
    "\n========== READ CLEAN ACTIVITY PARQUET =========="
)


clean_activity_parquet_read = (
    spark.read
    .parquet(
        str(clean_activity_parquet_path)
    )
)


clean_activity_read_count = (
    clean_activity_parquet_read.count()
)


print(
    "Rows read back:",
    clean_activity_read_count
)


logger.info(
    "Clean activity Parquet rows read back: %d",
    clean_activity_read_count
)


# ============================================================
# 17. VALIDATE CLEAN ACTIVITY COUNT
# ============================================================

if (
    clean_activity_read_count
    == clean_activity_count
):

    logger.info(
        "Clean activity Parquet count validation: PASS"
    )

else:

    logger.error(
        "Clean activity Parquet count validation: FAIL"
        " expected=%d actual=%d",
        clean_activity_count,
        clean_activity_read_count
    )


assert (
    clean_activity_read_count
    == clean_activity_count
)


# ============================================================
# 18. READ HOURLY GRID PARQUET BACK
# ============================================================

print(
    "\n========== READ HOURLY GRID PARQUET =========="
)


hourly_grid_parquet_read = (
    spark.read
    .parquet(
        str(hourly_grid_parquet_path)
    )
)


hourly_grid_read_count = (
    hourly_grid_parquet_read.count()
)


print(
    "Rows read back:",
    hourly_grid_read_count
)


logger.info(
    "Hourly grid Parquet rows read back: %d",
    hourly_grid_read_count
)


# ============================================================
# 19. VALIDATE HOURLY GRID COUNT
# ============================================================

if (
    hourly_grid_read_count
    == hourly_grid_count
):

    logger.info(
        "Hourly grid Parquet count validation: PASS"
    )

else:

    logger.error(
        "Hourly grid Parquet count validation: FAIL"
        " expected=%d actual=%d",
        hourly_grid_count,
        hourly_grid_read_count
    )


assert (
    hourly_grid_read_count
    == hourly_grid_count
)


# ============================================================
# 20. PRINT CLEAN ACTIVITY SCHEMA
# ============================================================

print(
    "\n========== CLEAN ACTIVITY PARQUET SCHEMA =========="
)

clean_activity_parquet_read.printSchema()


logger.info(
    "Clean activity Parquet schema inspected"
)


# ============================================================
# 21. PRINT HOURLY GRID SCHEMA
# ============================================================

print(
    "\n========== HOURLY GRID PARQUET SCHEMA =========="
)

hourly_grid_parquet_read.printSchema()


logger.info(
    "Hourly grid Parquet schema inspected"
)


# ============================================================
# 22. SCHEMA VALIDATION
# ============================================================

clean_schema_match = (
    clean_activity_parquet_read.schema
    == clean_activity_output.schema
)


hourly_schema_match = (
    hourly_grid_parquet_read.schema
    == hourly_grid_output.schema
)


print(
    "Clean activity schema match:",
    clean_schema_match
)

print(
    "Hourly grid schema match:",
    hourly_schema_match
)


logger.info(
    "Clean activity schema validation: %s",
    "PASS" if clean_schema_match else "FAIL"
)

logger.info(
    "Hourly grid schema validation: %s",
    "PASS" if hourly_schema_match else "FAIL"
)


# assert clean_schema_match

# assert hourly_schema_match


# ============================================================
# 23. CHECK DATE PARTITIONS
# ============================================================

print(
    "\n========== DATE PARTITION CHECK =========="
)


date_partition_count = (
    clean_activity_parquet_read
    .select("date")
    .distinct()
    .count()
)


print(
    "Distinct date partitions:",
    date_partition_count
)


logger.info(
    "Distinct date partitions in clean activity Parquet: %d",
    date_partition_count
)


# ============================================================
# 24. FILE SIZE HELPER
# ============================================================

def get_directory_size(path):
    """
    Return total size of all files inside a directory
    in bytes.
    """

    path = Path(path)

    if not path.exists():

        return 0

    total_size = 0

    for file_path in path.rglob("*"):

        if file_path.is_file():

            total_size += (
                file_path.stat().st_size
            )

    return total_size


def bytes_to_mb(size_bytes):

    return size_bytes / (
        1024 * 1024
    )


# ============================================================
# 25. PARQUET FILE SIZE
# ============================================================

clean_parquet_size = (
    get_directory_size(
        clean_activity_parquet_path
    )
)

hourly_parquet_size = (
    get_directory_size(
        hourly_grid_parquet_path
    )
)


print(
    "\n========== PARQUET FILE SIZES =========="
)

print(
    f"Clean activity Parquet: "
    f"{bytes_to_mb(clean_parquet_size):.2f} MB"
)

print(
    f"Hourly grid Parquet: "
    f"{bytes_to_mb(hourly_parquet_size):.2f} MB"
)


logger.info(
    "Clean activity Parquet size: %.2f MB",
    bytes_to_mb(clean_parquet_size)
)

logger.info(
    "Hourly grid Parquet size: %.2f MB",
    bytes_to_mb(hourly_parquet_size)
)


# ============================================================
# 26. OPTIONAL CSV SIZE COMPARISON
#
# To make a meaningful columnar-vs-row comparison, create
# a temporary CSV representation of the SAME clean activity
# data.
#
# This is NOT a required project output.
# It is only used for measuring storage size.
# ============================================================

comparison_csv_path = (
    curated_directory
    / "_sp6_csv_comparison"
)


print(
    "\n========== STORAGE COMPARISON =========="
)


(
    clean_activity_output
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(
        str(comparison_csv_path)
    )
)


csv_comparison_size = (
    get_directory_size(
        comparison_csv_path
    )
)


print(
    f"Equivalent CSV size: "
    f"{bytes_to_mb(csv_comparison_size):.2f} MB"
)

print(
    f"Parquet size: "
    f"{bytes_to_mb(clean_parquet_size):.2f} MB"
)


logger.info(
    "Equivalent CSV comparison size: %.2f MB",
    bytes_to_mb(csv_comparison_size)
)

logger.info(
    "Parquet size: %.2f MB",
    bytes_to_mb(clean_parquet_size)
)


# ============================================================
# 27. STORAGE REDUCTION
# ============================================================

if csv_comparison_size > 0:

    parquet_storage_ratio = (
        clean_parquet_size
        / csv_comparison_size
    )

    storage_reduction_percentage = (
        1
        - parquet_storage_ratio
    ) * 100

else:

    parquet_storage_ratio = 0

    storage_reduction_percentage = 0


print(
    f"Parquet/CSV size ratio: "
    f"{parquet_storage_ratio:.3f}"
)

print(
    f"Approximate storage reduction: "
    f"{storage_reduction_percentage:.2f}%"
)


logger.info(
    "Parquet/CSV size ratio: %.3f",
    parquet_storage_ratio
)

logger.info(
    "Approximate storage reduction using Parquet: %.2f%%",
    storage_reduction_percentage
)


# ============================================================
# 28. REMOVE TEMPORARY CSV COMPARISON
#
# The project does NOT require this CSV.
# ============================================================

if comparison_csv_path.exists():

    shutil.rmtree(
        comparison_csv_path
    )


logger.info(
    "Temporary CSV comparison output removed"
)


# ============================================================
# 29. COLUMNAR STORAGE OBSERVATION
# ============================================================

logger.info(
    "COLUMNAR STORAGE OBSERVATION: Parquet stores data "
    "column-wise, allowing Spark to read only the columns "
    "required by a query instead of scanning every field."
)


logger.info(
    "COLUMNAR STORAGE OBSERVATION: Parquet supports "
    "compression and efficient encoding, which can reduce "
    "storage size and I/O compared with plain CSV."
)


logger.info(
    "COLUMNAR STORAGE OBSERVATION: The observed Parquet "
    "size was %.2f MB compared with %.2f MB for the "
    "equivalent CSV representation.",
    bytes_to_mb(clean_parquet_size),
    bytes_to_mb(csv_comparison_size)
)


# ============================================================
# 30. PARTITIONING OBSERVATION
# ============================================================

logger.info(
    "PARTITIONING OBSERVATION: Clean activity data is "
    "partitioned by date, allowing downstream queries "
    "restricted to specific dates to avoid scanning "
    "unnecessary date partitions."
)


# ============================================================
# 31. GEOMETRY STORAGE OBSERVATION
# ============================================================

logger.info(
    "GEOMETRY STORAGE OBSERVATION: Full Polygon geometry "
    "is retained in the static milano-grid.geojson "
    "reference rather than duplicated across every "
    "hourly analytics record."
)


# ============================================================
# 32. FINAL OUTPUT VALIDATION
# ============================================================

final_validation_pass = (
    clean_activity_read_count
    == clean_activity_count
    and hourly_grid_read_count
    == hourly_grid_count
    and clean_schema_match
    and hourly_schema_match
    and hourly_duplicate_groups == 0
    and reference_geojson_path.exists()
)


if final_validation_pass:

    sp6_status = "PASS"

else:

    sp6_status = "REVIEW"


logger.info(
    "Final SP6 validation status: %s",
    sp6_status
)


# ============================================================
# 33. FINAL LOG SUMMARY
# ============================================================

logger.info("=" * 60)
logger.info("SP6 FINAL SUMMARY")
logger.info("=" * 60)

logger.info(
    "Clean activity Parquet: CREATED"
)

logger.info(
    "Clean activity partitioning: date"
)

logger.info(
    "hourly_grid_summary Parquet: CREATED"
)

logger.info(
    "Hourly grid uniqueness: one record per grid_id + timestamp"
)

logger.info(
    "Geometry duplication in analytics: NOT PRESENT"
)

logger.info(
    "Static GeoJSON reference: RETAINED"
)

logger.info(
    "Dashboard CSV: CREATED"
)

logger.info(
    "Clean activity read-back count validation: %s",
    "PASS"
    if clean_activity_read_count == clean_activity_count
    else "FAIL"
)

logger.info(
    "Hourly grid read-back count validation: %s",
    "PASS"
    if hourly_grid_read_count == hourly_grid_count
    else "FAIL"
)

logger.info(
    "Clean activity schema validation: %s",
    "PASS"
    if clean_schema_match
    else "FAIL"
)

logger.info(
    "Hourly grid schema validation: %s",
    "PASS"
    if hourly_schema_match
    else "FAIL"
)

logger.info(
    "Parquet columnar storage comparison completed"
)

logger.info(
    "SP6 STATUS: %s",
    sp6_status
)

logger.info("=" * 60)


# ============================================================
# 34. CONSOLE SUMMARY
# ============================================================

print(
    "\n" + "=" * 60
)

print(
    "                 SP6 SUMMARY"
)

print(
    "=" * 60
)

print(
    f"Clean activity rows       : {clean_activity_count}"
)

print(
    f"Hourly grid rows          : {hourly_grid_count}"
)

print(
    f"Date partitions           : {date_partition_count}"
)

print(
    f"Clean Parquet size        : "
    f"{bytes_to_mb(clean_parquet_size):.2f} MB"
)

print(
    f"Hourly Parquet size       : "
    f"{bytes_to_mb(hourly_parquet_size):.2f} MB"
)

print(
    f"Equivalent CSV size       : "
    f"{bytes_to_mb(csv_comparison_size):.2f} MB"
)

print(
    f"Storage reduction         : "
    f"{storage_reduction_percentage:.2f}%"
)

print(
    f"Static GeoJSON            : "
    f"{reference_geojson_path}"
)

print(
    f"Dashboard CSV             : "
    f"{dashboard_csv_path}"
)

print(
    f"Final validation          : "
    f"{sp6_status}"
)

print(
    f"Log                       : "
    f"{log_file}"
)

print(
    "=" * 60
)