# ============================================================
# PHASE 2 - SP4
# Geographic Enrichment
# ============================================================

import json
import logging
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
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

log_file = log_directory / "sp4.log"


logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("SP4")


logger.info("=" * 60)
logger.info("SP4 START")
logger.info("=" * 60)


print("\n" + "=" * 60)
print("                 SP4 START")
print("=" * 60)


# ============================================================
# 2. GEOJSON PATH
# ============================================================

geojson_path = Path(
    "D:/capstone1/data/milano-grid.geojson" \
    ""
)

hourly_grid_summary = spark.read.parquet(
    "D:/capstone1/parquet_output/hourly_grid_parquet"
)


if not geojson_path.exists():

    logger.error(
        "GeoJSON file not found: %s",
        geojson_path
    )

    raise FileNotFoundError(
        f"GeoJSON file not found: {geojson_path}"
    )


logger.info(
    "GeoJSON file found: %s",
    geojson_path
)


# ============================================================
# 3. LOAD GEOJSON
# ============================================================

with open(
    geojson_path,
    "r",
    encoding="utf-8"
) as file:

    geojson_data = json.load(file)


# ============================================================
# 4. INSPECT TOP-LEVEL GEOJSON STRUCTURE
# ============================================================

top_level_type = geojson_data.get(
    "type"
)

features = geojson_data.get(
    "features",
    []
)


print("\n========== GEOJSON STRUCTURE ==========")

print(
    "Top-level type:",
    top_level_type
)

print(
    "Number of features:",
    len(features)
)


logger.info(
    "GeoJSON top-level type: %s",
    top_level_type
)

logger.info(
    "GeoJSON feature count: %d",
    len(features)
)


# ============================================================
# 5. VALIDATE TOP-LEVEL TYPE
# ============================================================

if top_level_type != "FeatureCollection":

    logger.error(
        "Unexpected GeoJSON top-level type: %s",
        top_level_type
    )

    raise ValueError(
        "Expected GeoJSON top-level type "
        "'FeatureCollection'"
    )


logger.info(
    "GeoJSON top-level type validation: PASS"
)


# ============================================================
# 6. INSPECT FIRST FEATURE
# ============================================================

if not features:

    logger.error(
        "GeoJSON contains no features"
    )

    raise ValueError(
        "GeoJSON contains no features"
    )


first_feature = features[0]

first_properties = first_feature.get(
    "properties",
    {}
)

first_geometry = first_feature.get(
    "geometry",
    {}
)


print("\n========== FIRST GEOJSON FEATURE ==========")

print(
    "Feature type:",
    first_feature.get("type")
)

print(
    "Property keys:",
    list(first_properties.keys())
)

print(
    "cellId:",
    first_properties.get("cellId")
)

print(
    "Geometry type:",
    first_geometry.get("type")
)


logger.info(
    "First feature type: %s",
    first_feature.get("type")
)

logger.info(
    "GeoJSON property keys: %s",
    list(first_properties.keys())
)

logger.info(
    "GeoJSON identifier field: properties.cellId"
)

logger.info(
    "First feature geometry type: %s",
    first_geometry.get("type")
)


# ============================================================
# 7. IDENTIFY GEOMETRY TYPES
# ============================================================

geometry_types = set()

for feature in features:

    geometry = feature.get(
        "geometry"
    )

    if geometry is not None:

        geometry_types.add(
            geometry.get("type")
        )


print(
    "\nGeometry types found:",
    geometry_types
)


logger.info(
    "Geometry types found: %s",
    geometry_types
)


# ============================================================
# 8. CREATE GRID LOOKUP
#
# GeoJSON:
#
# properties.cellId
#       ↓
# grid_id
#
# geometry
# ============================================================

grid_records = []

for feature in features:

    properties = feature.get(
        "properties",
        {}
    )

    geometry = feature.get(
        "geometry"
    )

    cell_id = properties.get(
        "cellId"
    )

    # --------------------------------------------------------
    # Skip features without cellId
    # --------------------------------------------------------

    if cell_id is None:

        logger.warning(
            "GeoJSON feature missing properties.cellId"
        )

        continue

    # --------------------------------------------------------
    # Convert geometry back to JSON string.
    #
    # Keeping geometry as a JSON string makes the lookup
    # portable inside Spark without requiring a spatial
    # Spark library.
    # --------------------------------------------------------

    geometry_json = json.dumps(
        geometry,
        separators=(",", ":")
    )

    grid_records.append(
        (
            str(cell_id),
            geometry_json
        )
    )


# ============================================================
# 9. CREATE SPARK DATAFRAME FOR GRID LOOKUP
# ============================================================

grid_schema = StructType([
    StructField(
        "grid_id",
        StringType(),
        False
    ),

    StructField(
        "geometry",
        StringType(),
        True
    )
])


grid_lookup = spark.createDataFrame(
    grid_records,
    schema=grid_schema
)


grid_lookup = grid_lookup.dropDuplicates(
    ["grid_id"]
)


# ============================================================
# 10. INSPECT GRID LOOKUP SIZE
# ============================================================

grid_lookup_count = grid_lookup.count()

print(
    "\n========== GRID LOOKUP =========="
)

print(
    "Grid lookup rows:",
    grid_lookup_count
)

print(
    "Activity DataFrame rows:",
    hourly_grid_summary.count()
)


logger.info(
    "Grid lookup rows: %d",
    grid_lookup_count
)

logger.info(
    "Activity DataFrame rows: %d",
    hourly_grid_summary.count()
)


# ============================================================
# 11. VALIDATE EXPECTED GRID LOOKUP SIZE
#
# Project expectation:
# approximately 10,000 grid records.
# ============================================================

if grid_lookup_count == 10000:

    logger.info(
        "Grid lookup size check: PASS - 10,000 grids"
    )

else:

    logger.warning(
        "Grid lookup size is %d; expected approximately 10,000",
        grid_lookup_count
    )


# ============================================================
# 12. CHECK GRID LOOKUP DUPLICATES
# ============================================================

duplicate_grid_lookup_count = (
    grid_lookup
    .groupBy("grid_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


logger.info(
    "Duplicate grid IDs in lookup: %d",
    duplicate_grid_lookup_count
)


assert duplicate_grid_lookup_count == 0, (
    "Grid lookup contains duplicate grid_id values"
)


# ============================================================
# 13. ACTIVITY DATA DISTINCT GRID COUNT
# ============================================================

activity_grid_count_before = (
    hourly_grid_summary
    .select("grid_id")
    .distinct()
    .count()
)


print(
    "\nActivity distinct grids before join:",
    activity_grid_count_before
)


logger.info(
    "Distinct activity grids before join: %d",
    activity_grid_count_before
)


# ============================================================
# 14. GRID IDS PRESENT IN GEOJSON
# ============================================================

geojson_grid_count = (
    grid_lookup
    .select("grid_id")
    .distinct()
    .count()
)


logger.info(
    "Distinct GeoJSON grid IDs: %d",
    geojson_grid_count
)


# ============================================================
# 15. COMPARE GRID ID TYPES
# ============================================================

logger.info(
    "Common join key:"
    " activity.grid_id <-> GeoJSON.properties.cellId"
)

logger.info(
    "GeoJSON properties.cellId normalized to grid_id"
)


# ============================================================
# 16. STANDARD JOIN EXECUTION PLAN
# ============================================================

print(
    "\n========== STANDARD JOIN PLAN =========="
)

standard_join_df = (
    hourly_grid_summary
    .join(
        grid_lookup,
        on="grid_id",
        how="left"
    )
)


standard_join_df.explain(
    mode="formatted"
)


logger.info(
    "Standard join execution plan inspected"
)


# ============================================================
# 17. BROADCAST JOIN
# ============================================================

from pyspark.sql.functions import broadcast


grid_lookup_broadcast = broadcast(
    grid_lookup
)


grid_activity_geo_df = (
    hourly_grid_summary
    .join(
        grid_lookup_broadcast,
        on="grid_id",
        how="left"
    )
)


print(
    "\n========== BROADCAST JOIN PLAN =========="
)

grid_activity_geo_df.explain(
    mode="formatted"
)


logger.info(
    "Broadcast join execution plan inspected"
)

logger.info(
    "Grid lookup is a broadcast candidate because it "
    "contains approximately 10,000 rows versus millions "
    "of activity records"
)


# ============================================================
# 18. CACHE ENRICHED DATASET
# ============================================================

grid_activity_geo_df = (
    grid_activity_geo_df.cache()
)


enriched_row_count = (
    grid_activity_geo_df.count()
)


logger.info(
    "Enriched activity rows: %d",
    enriched_row_count
)


# ============================================================
# 19. DISTINCT ACTIVITY GRIDS AFTER JOIN
# ============================================================

activity_grid_count_after = (
    grid_activity_geo_df
    .select("grid_id")
    .distinct()
    .count()
)


print(
    "\nDistinct activity grids after join:",
    activity_grid_count_after
)


logger.info(
    "Distinct activity grids after join: %d",
    activity_grid_count_after
)


# ============================================================
# 20. MISSING GEOMETRY CHECK
# ============================================================

missing_geometry_grid_df = (
    grid_activity_geo_df
    .filter(
        F.col("geometry").isNull()
    )
    .select("grid_id")
    .distinct()
)


missing_geometry_count = (
    missing_geometry_grid_df.count()
)


print(
    "Activity grids with missing geometry:",
    missing_geometry_count
)


logger.info(
    "Activity grids with missing geometry: %d",
    missing_geometry_count
)


# ============================================================
# 21. SUCCESSFUL ENRICHMENT PERCENTAGE
# ============================================================

if activity_grid_count_before > 0:

    enrichment_percentage = (
        (
            activity_grid_count_before
            - missing_geometry_count
        )
        / activity_grid_count_before
    ) * 100

else:

    enrichment_percentage = 0.0


print(
    f"Geometry enrichment coverage: "
    f"{enrichment_percentage:.2f}%"
)


logger.info(
    "Geometry enrichment coverage: %.2f%%",
    enrichment_percentage
)


# ============================================================
# 22. UNMATCHED GRID IDs
# ============================================================

unmatched_grid_ids_df = (
    missing_geometry_grid_df
    .orderBy("grid_id")
)


unmatched_grid_count = (
    unmatched_grid_ids_df.count()
)


print(
    "\n========== UNMATCHED GRID IDS =========="
)

print(
    "Unmatched grid IDs:",
    unmatched_grid_count
)


if unmatched_grid_count > 0:

    unmatched_grid_ids_df.show(
        100,
        truncate=False
    )

    logger.warning(
        "Unmatched grid IDs found: %d",
        unmatched_grid_count
    )

else:

    logger.info(
        "No unmatched grid IDs found"
    )


# ============================================================
# 23. NUMERICAL JOIN VALIDATION
#
# Left join must preserve every activity row.
# ============================================================

if enriched_row_count == hourly_grid_summary.count():

    logger.info(
        "Numerical row preservation check: PASS"
    )

else:

    logger.error(
        "Numerical row preservation check: FAIL. "
        "Before=%d, After=%d",
        hourly_grid_summary.count(),
        enriched_row_count
    )


assert enriched_row_count == hourly_grid_summary.count(), (
    "Left join changed the activity row count"
)


# ============================================================
# 24. DISTINCT GRID VALIDATION
# ============================================================

if activity_grid_count_after == activity_grid_count_before:

    logger.info(
        "Distinct activity grid preservation: PASS"
    )

else:

    logger.error(
        "Distinct activity grid preservation: FAIL"
    )


assert activity_grid_count_after == activity_grid_count_before


# ============================================================
# 25. GEOGRAPHIC VALIDATION
#
# Check:
#   1. Geometry exists
#   2. Geometry type is consistent
#   3. Geometry contains coordinates
#   4. No empty geometry object
# ============================================================

print(
    "\n========== GEOGRAPHIC VALIDATION =========="
)


# ------------------------------------------------------------
# Geometry JSON validation
# ------------------------------------------------------------

geometry_type_counts = (
    grid_lookup
    .select(
        F.get_json_object(
            "geometry",
            "$.type"
        ).alias(
            "geometry_type"
        )
    )
    .groupBy(
        "geometry_type"
    )
    .count()
    .orderBy(
        "geometry_type"
    )
)


geometry_type_counts.show(
    truncate=False
)


geometry_type_rows = (
    geometry_type_counts.collect()
)


geometry_type_summary = {}

for row in geometry_type_rows:

    geometry_type_summary[
        str(row["geometry_type"])
    ] = int(row["count"])


logger.info(
    "GeoJSON geometry type distribution: %s",
    geometry_type_summary
)


# ============================================================
# 26. CHECK EMPTY / MISSING GEOMETRY
# ============================================================

empty_geometry_count = (
    grid_activity_geo_df
    .filter(
        F.col("geometry").isNull()
        | (
            F.length(
                F.trim(
                    F.col("geometry")
                )
            ) == 0
        )
    )
    .select("grid_id")
    .distinct()
    .count()
)


logger.info(
    "Distinct grids with empty/missing geometry: %d",
    empty_geometry_count
)


# ============================================================
# 27. CHECK GEOJSON GEOMETRY OBJECT TYPE
# ============================================================

invalid_geometry_object_count = (
    grid_activity_geo_df
    .filter(
        F.get_json_object(
            "geometry",
            "$.type"
        ).isNull()
    )
    .select("grid_id")
    .distinct()
    .count()
)


logger.info(
    "Distinct grids with invalid geometry JSON/type: %d",
    invalid_geometry_object_count
)


# ============================================================
# 28. GEOGRAPHIC VALIDATION RESULT
# ============================================================

geographic_validation_pass = (
    missing_geometry_count == 0
    and empty_geometry_count == 0
    and invalid_geometry_object_count == 0
)


if geographic_validation_pass:

    logger.info(
        "Geographic validation: PASS"
    )

else:

    logger.warning(
        "Geographic validation: FAIL / REVIEW REQUIRED"
    )


print(
    "Geographic validation:",
    "PASS"
    if geographic_validation_pass
    else "FAIL / REVIEW"
)


# ============================================================
# 29. CREATE FINAL REQUIRED COLUMNS
#
# Expected:
#
# timestamp
# grid_id
# sms_in
# sms_out
# call_in
# call_out
# internet_activity
# total_activity
# geometry
# ============================================================

grid_activity_geo_df = (
    grid_activity_geo_df
    .select(
        "timestamp",
        "grid_id",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
        "total_activity",
        "geometry"
    )
)


# Re-cache because select created a new DataFrame.
grid_activity_geo_df = (
    grid_activity_geo_df.cache()
)


final_enriched_count = (
    grid_activity_geo_df.count()
)


logger.info(
    "Final grid_activity_geo_df rows: %d",
    final_enriched_count
)


# ============================================================
# 30. TOP HIGH-ACTIVITY GRIDS
#
# Selected window:
# None = entire dataset
# ============================================================

selected_start_date = None
selected_end_date = None


top_activity_window_df = (
    grid_activity_geo_df
)


if selected_start_date is not None:

    top_activity_window_df = (
        top_activity_window_df
        .filter(
            F.to_date("timestamp")
            >= F.lit(selected_start_date)
        )
    )


if selected_end_date is not None:

    top_activity_window_df = (
        top_activity_window_df
        .filter(
            F.to_date("timestamp")
            <= F.lit(selected_end_date)
        )
    )


# ============================================================
# 31. HOTSPOT RANKING WITH GEOMETRY
# ============================================================

hotspot_ranking = (
    top_activity_window_df
    .groupBy(
        "grid_id",
        "geometry"
    )
    .agg(
        F.sum(
            "total_activity"
        ).alias(
            "window_total_activity"
        )
    )
    .orderBy(
        F.desc(
            "window_total_activity"
        )
    )
    .limit(10)
)


print(
    "\n========== TOP 10 HIGH-ACTIVITY GRIDS =========="
)

hotspot_ranking.show(
    10,
    truncate=False
)


logger.info(
    "Created hotspot ranking with geometry retained"
)


# ============================================================
# 32. OPTIONAL CENTROID DERIVATION
#
# This does not calculate a true GIS centroid.
#
# It provides a simple coordinate-based centroid for
# Polygon/MultiPolygon GeoJSON structures when possible.
#
# For production GIS accuracy, a spatial library such as
# Apache Sedona should be used.
# ============================================================

logger.info(
    "Centroid derivation is optional and not enabled by default"
)


# ============================================================
# 33. FINAL SCHEMA
# ============================================================

print(
    "\n========== GRID ACTIVITY GEO SCHEMA =========="
)

grid_activity_geo_df.printSchema()


logger.info(
    "Final grid_activity_geo_df schema inspected"
)


# ============================================================
# 34. FINAL SAMPLE
# ============================================================

print(
    "\n========== GRID ACTIVITY GEO SAMPLE =========="
)

grid_activity_geo_df.show(
    10,
    truncate=False
)


grid_activity_geo_df.write.mode("overwrite").parquet(
    "D:/capstone1/parquet_output/grid_activity_geo_parquet"
)

grid_lookup.write.mode("overwrite").parquet(
    "D:/capstone1/parquet_output/grid_lookup_parquet"
)


# ============================================================
# 35. FINAL SP4 STATUS
# ============================================================

numerical_join_pass = (
    enriched_row_count
    == hourly_grid_summary.count()
)

grid_preservation_pass = (
    activity_grid_count_after
    == activity_grid_count_before
)

sp4_status = (
    "PASS"
    if (
        numerical_join_pass
        and grid_preservation_pass
        and geographic_validation_pass
    )
    else "REVIEW"
)


# ============================================================
# 36. WRITE FINAL LOG
# ============================================================

logger.info("=" * 60)
logger.info("SP4 FINAL SUMMARY")
logger.info("=" * 60)

logger.info(
    "GeoJSON top-level type: %s",
    top_level_type
)

logger.info(
    "GeoJSON features: %d",
    len(features)
)

logger.info(
    "Grid lookup rows: %d",
    grid_lookup_count
)

logger.info(
    "Activity rows before join: %d",
    hourly_grid_summary.count()
)

logger.info(
    "Activity rows after join: %d",
    enriched_row_count
)

logger.info(
    "Distinct activity grids before join: %d",
    activity_grid_count_before
)

logger.info(
    "Distinct activity grids after join: %d",
    activity_grid_count_after
)

logger.info(
    "Missing geometry grids: %d",
    missing_geometry_count
)

logger.info(
    "Enrichment coverage: %.2f%%",
    enrichment_percentage
)

logger.info(
    "Unmatched grid IDs: %d",
    unmatched_grid_count
)

logger.info(
    "Empty geometry grids: %d",
    empty_geometry_count
)

logger.info(
    "Invalid geometry object grids: %d",
    invalid_geometry_object_count
)

logger.info(
    "Numerical join validation: %s",
    "PASS"
    if numerical_join_pass
    else "FAIL"
)

logger.info(
    "Grid preservation validation: %s",
    "PASS"
    if grid_preservation_pass
    else "FAIL"
)

logger.info(
    "Geographic validation: %s",
    "PASS"
    if geographic_validation_pass
    else "REVIEW"
)

logger.info(
    "Expected output: grid_activity_geo_df - CREATED"
)

logger.info(
    "Expected output: grid enrichment coverage report - CREATED IN LOG"
)

logger.info(
    "Expected output: unmatched grid_id values - CREATED"
)

logger.info(
    "Expected output: top high-activity grids with geometry - CREATED"
)

logger.info(
    "Processed output suitable for map visualization - CREATED"
)

logger.info(
    "SP4 STATUS: %s",
    sp4_status
)

logger.info("=" * 60)


# ============================================================
# 37. CONSOLE SUMMARY
# ============================================================

print("\n" + "=" * 60)
print("                 SP4 SUMMARY")
print("=" * 60)

print(
    f"GeoJSON features          : {len(features)}"
)

print(
    f"Grid lookup rows          : {grid_lookup_count}"
)

print(
    f"Activity rows before join : {hourly_grid_summary.count()}"
)

print(
    f"Activity rows after join  : {enriched_row_count}"
)

print(
    f"Distinct grids before    : {activity_grid_count_before}"
)

print(
    f"Distinct grids after     : {activity_grid_count_after}"
)

print(
    f"Missing geometry grids   : {missing_geometry_count}"
)

print(
    f"Enrichment coverage      : "
    f"{enrichment_percentage:.2f}%"
)

print(
    f"Unmatched grid IDs       : {unmatched_grid_count}"
)

print(
    f"Geographic validation    : "
    f"{'PASS' if geographic_validation_pass else 'REVIEW'}"
)

print(
    f"Numerical validation     : "
    f"{'PASS' if numerical_join_pass else 'FAIL'}"
)

print(
    f"SP4 status               : {sp4_status}"
)

print("=" * 60)

print(
    f"\nSP4 log saved to: {log_file}"
)