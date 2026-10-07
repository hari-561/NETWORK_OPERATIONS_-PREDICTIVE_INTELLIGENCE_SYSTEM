# ============================================================
# PHASE 2 - SP2
# Canonicalization, Cleaning and Feature Derivation
# ============================================================
import json
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import glob

os.environ["HADOOP_HOME"] = r"D:\capstone1\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";D:\capstone1\hadoop\bin"
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NetworkProject1")
    .master("local[1]")
    .getOrCreate()
)


# ============================================================
# 1. PRESERVE RAW DATAFRAME
# ============================================================

# df is the DataFrame produced by SP1

raw_df =  spark.read.parquet(
    "D:/capstone1/parquet_output/raw_parquet"
)

print("========== SP2 INPUT CHECK ==========")


print("Raw schema:")
raw_df.printSchema()

raw_count = raw_df.count()

print("Rows before cleaning:", raw_count)


# ============================================================
# 2. CREATE CLEAN DATAFRAME
# ============================================================

# Spark DataFrames are immutable.
# Transforming clean_df will not modify raw_df.

clean_df = raw_df

# ============================================================
# 3. RENAME RAW COLUMNS TO CANONICAL NAMES
# ============================================================

rename_mapping = {
    "datetime":"timestamp",
    "CellID": "grid_id",
    "countrycode": "country_code",
    "smsin": "sms_in",
    "smsout": "sms_out",
    "callin": "call_in",
    "callout": "call_out",
    "internet": "internet_activity"
}

for raw_column, canonical_column in rename_mapping.items():
    clean_df = clean_df.withColumnRenamed(
        raw_column,
        canonical_column
    )


print("cheaned schema:")
clean_df.printSchema()



# ============================================================
# 4. CAST TIMESTAMP TO DATETIME
# ============================================================

clean_df = clean_df.withColumn(
    "timestamp",
    F.to_timestamp(F.col("timestamp"))
)


# ============================================================
# 5. CAST ACTIVITY MEASURES TO NUMERIC TYPES
# ============================================================

activity_columns = [
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity"
]

for column_name in activity_columns:
    clean_df = clean_df.withColumn(
        column_name,
        F.col(column_name).cast("double")
    )




# ============================================================
# 6. PROFILE ACTIVITY NULLS BEFORE NULL → ZERO
# ============================================================
raw_activity_columns =[
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet"]

print("\n========== RAW ACTIVITY NULL PROFILE ==========")

activity_null_counts = {}

for column_name in raw_activity_columns:

    null_count = raw_df.filter(
        F.col(column_name).isNull()
    ).count()

    activity_null_counts[column_name] = null_count

    print(
        f"{column_name}: {null_count}"
    )


total_activity_nulls = sum(
    activity_null_counts.values()
)

print(
    "Total activity nulls:",
    total_activity_nulls
)


# ============================================================
# 7. IDENTIFY INVALID ROWS
#
# Invalid if:
#   - grid_id is missing
#   - timestamp is missing
#   - any activity value is negative
# ============================================================




invalid_condition = (
    F.col("grid_id").isNull()
    | F.col("timestamp").isNull()
    | (F.col("sms_in").isNotNull() & (F.col("sms_in") < 0))
    | (F.col("sms_out").isNotNull() & (F.col("sms_out") < 0))
    | (F.col("call_in").isNotNull() & (F.col("call_in") < 0))
    | (F.col("call_out").isNotNull() & (F.col("call_out") < 0))
    | (F.col("internet_activity").isNotNull() & (F.col("internet_activity") < 0))
)


# for column_name in activity_columns:

#     invalid_condition = (
#         invalid_condition
#         | (F.col(column_name).isNotNull() & F.col(column_name) < 0)
#     )


# ============================================================
# 8. CREATE REJECTED / QUARANTINE DATAFRAME
# ============================================================


rejected_df = clean_df.filter(
    invalid_condition
)



# ============================================================
# 9. CREATE VALID CLEAN DATAFRAME
# ============================================================

clean_df = clean_df.filter(
    ~invalid_condition
)



# ============================================================
# 10. COUNT REJECTED ROWS
# ============================================================

rejected_count = rejected_df.count()

print("\n========== REJECTION SUMMARY ==========")

print(
    "Rows rejected:",
    rejected_count
)


# ============================================================
# 11. APPLY CURATED NULL → ZERO RULE
#
# IMPORTANT:
# This happens AFTER raw activity null profiling.
# raw_df remains untouched.
# ============================================================

for column_name in activity_columns:

    clean_df = clean_df.withColumn(
        column_name,
        F.coalesce(
            F.col(column_name),
            F.lit(0.0)
        )
    )




# ============================================================
# 12. CREATE TOTAL SMS
# ============================================================

clean_df = clean_df.withColumn(
    "total_sms",
    F.col("sms_in") + F.col("sms_out")
)



# ============================================================
# 13. CREATE TOTAL CALLS
# ============================================================

clean_df = clean_df.withColumn(
    "total_calls",
    F.col("call_in") + F.col("call_out")
)


# ============================================================
# 14. CREATE TOTAL ACTIVITY
#
# Project-defined indicator:
# total_sms + total_calls + internet
# ============================================================

clean_df = clean_df.withColumn(
    "total_activity",
    F.col("total_sms")
    + F.col("total_calls")
    + F.col("internet_activity")
)


# ============================================================
# 15. DERIVE DATE
# ============================================================

clean_df = clean_df.withColumn(
    "date",
    F.to_date(F.col("timestamp"))
)


# ============================================================
# 16. DERIVE HOUR
# ============================================================

clean_df = clean_df.withColumn(
    "hour",
    F.hour(F.col("timestamp"))
)



# ============================================================
# 17. DERIVE DAY OF WEEK
#
# Spark:
# 1 = Sunday
# 2 = Monday
# ...
# 7 = Saturday
# ============================================================

clean_df = clean_df.withColumn(
    "day_of_week",
    F.dayofweek(F.col("timestamp"))
)


# ============================================================
# 18. VERIFY HOURLY CADENCE
#
# Check cadence separately within each source file.
# Expected difference = 3600 seconds = 1 hour.
# ============================================================

cadence_window = (
    Window
    .partitionBy("source_file")
    .orderBy("timestamp")
)

cadence_df = (
    clean_df
    .select(
        "source_file",
        "timestamp"
    )
    .distinct()
    .withColumn(
        "previous_timestamp",
        F.lag("timestamp").over(cadence_window)
    )
    .withColumn(
        "difference_seconds",
        F.unix_timestamp("timestamp")
        - F.unix_timestamp("previous_timestamp")
    )
)


# ============================================================
# 19. FIND CADENCE VIOLATIONS
# ============================================================

cadence_violations = cadence_df.filter(
    F.col("difference_seconds").isNotNull()
    & (F.col("difference_seconds") != 3600)
)


cadence_violation_count = cadence_violations.count()


print("\n========== HOURLY CADENCE CHECK ==========")

print(
    "Cadence violations:",
    cadence_violation_count
)


if cadence_violation_count == 0:
    print("Hourly cadence check: PASS")
else:
    print("Hourly cadence check: FAIL")

    cadence_violations.show(
        truncate=False
    )


# ============================================================
# 20. COUNT CLEAN ROWS
# ============================================================

clean_count = clean_df.count()


# ============================================================
# 21. VERIFY ROW COUNT BALANCE
#
# Before = Valid + Rejected
# ============================================================

print("\n========== ROW COUNT CHECK ==========")

print(
    "Rows before cleaning:",
    raw_count
)

print(
    "Rows rejected:",
    rejected_count
)

print(
    "Rows after cleaning:",
    clean_count
)

print(
    "Rejected + Clean:",
    rejected_count + clean_count
)


assert raw_count == (
    rejected_count + clean_count
), "Row count mismatch!"


print("Row count check: PASS")


# ============================================================
# 22. FINAL SP2 SUMMARY
# ============================================================

print("\n")
print("=" * 60)
print("                 SP2 SUMMARY")
print("=" * 60)

print(
    f"Rows before cleaning       : {raw_count}"
)

print(
    f"Rows rejected              : {rejected_count}"
)

print(
    f"Rows after cleaning        : {clean_count}"
)

print(
    f"Total activity nulls found : {total_activity_nulls}"
)

print(
    f"Cadence violations         : {cadence_violation_count}"
)

print(
    f"Row count check            : PASS"
)

print(
    f"Hourly cadence check       : "
    f"{'PASS' if cadence_violation_count == 0 else 'FAIL'}"
)

print("=" * 60)


# ============================================================
# 23. INSPECT FINAL CLEAN DATA
# ============================================================

print("\n========== FINAL CLEAN SCHEMA ==========")

clean_df.printSchema()


print("\n========== FINAL CLEAN DATA ==========")

clean_df.show(
    10,
    truncate=False
)


# ============================================================
# 24. INSPECT REJECTED DATA
# ============================================================

print("\n========== REJECTED DATA ==========")

rejected_df.show(
    10,
    truncate=False
)


clean_df.write.mode("overwrite").parquet(
    "D:/capstone1/parquet_output/clean_parquet"
)





# ============================================================
# SAVE SP2 SUMMARY AS JSON
# ============================================================

summary = {
    "phase": "Phase 2",
    "step": "SP2",
    "status": "PASS" if (
        cadence_violation_count == 0
        and raw_count == rejected_count + clean_count
    ) else "FAIL",

    "row_counts": {
        "before_cleaning": int(raw_count),
        "rejected": int(rejected_count),
        "after_cleaning": int(clean_count)
    },

    "activity_nulls": {
        "sms_in": int(activity_null_counts["smsin"]),
        "sms_out": int(activity_null_counts["smsout"]),
        "call_in": int(activity_null_counts["callin"]),
        "call_out": int(activity_null_counts["callout"]),
        "internet_activity": int(activity_null_counts["internet"]),
        "total": int(total_activity_nulls)
    },

    "validation": {
        "row_count_check": (
            raw_count == rejected_count + clean_count
        ),
        "hourly_cadence_check": (
            cadence_violation_count == 0
        ),
        "cadence_violations": int(
            cadence_violation_count
        )
    },

    "derived_columns": [
        "total_sms",
        "total_calls",
        "total_activity",
        "date",
        "hour",
        "day_of_week"
    ]
}


# ============================================================
# CREATE OUTPUT DIRECTORY
# ============================================================

output_dir = Path("D:/capstone1/phase2")
output_dir.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# WRITE JSON
# ============================================================

summary_path = output_dir / "sp2_summary.json"

with open(
    summary_path,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        summary,
        f,
        indent=4
    )


print(
    f"\nSP2 summary saved to: {summary_path}"
)


