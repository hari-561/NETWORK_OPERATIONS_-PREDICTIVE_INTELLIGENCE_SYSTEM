import os
import glob
import json

os.environ["HADOOP_HOME"] = r"D:\capstone1\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";D:\capstone1\hadoop\bin"
from pyspark.sql import SparkSession

from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    IntegerType,
    DoubleType
)
from pyspark.sql.functions import input_file_name,date_trunc

spark = (
    SparkSession.builder
    .appName("NetworkProject1")
    .master("local[1]")
    .getOrCreate()
)

 
DATA_FOLDER = "D:/capstone1/data"
 
FILE_PATTERN = os.path.join(
    DATA_FOLDER,
    "sms-call-internet-mi-*.csv"
)


# df = (
#     spark.read
#     .option("header", True)
#     .option("inferSchema", True)
#     .csv(input_path)
# )
 
actual_files = glob.glob(FILE_PATTERN)
 
print("\nFiles found:")
for file in actual_files:
    print(os.path.basename(file))
 
print("\nActual file count:", len(actual_files))
 

schema = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("CellID", IntegerType(), True),
    StructField("countrycode", IntegerType(), True),
    StructField("smsin", DoubleType(), True),
    StructField("smsout", DoubleType(), True),
    StructField("callin", DoubleType(), True),
    StructField("callout", DoubleType(), True),
    StructField("internet", DoubleType(), True)
])

df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(FILE_PATTERN)
)

print("csv read with struct feilds.....")


row_count = df.count()

print("Total rows:", row_count)


df = df.withColumn(
    "source_file",
    input_file_name()
)

source_file_count = df.select("source_file").distinct().count()

print("Source files:", source_file_count)

df.select("source_file").distinct().show(truncate=False)



unique_grids = df.select("CellID").distinct().count()

print("Unique grids:", unique_grids)



country_codes = df.select("countrycode").distinct().count()

print("Country-code categories:", country_codes)

#df.select("countrycode").distinct().show()


hourly_intervals = (
    df.select("datetime")
      .distinct()
      .count()
)

print("Distinct hourly intervals:", hourly_intervals)


df.select("datetime").distinct().orderBy("datetime").show(
    100,
    truncate=False
)

df = df.withColumn(
    "hour",
    date_trunc("hour", "datetime")
)

hourly_intervals = df.select("hour").distinct().count()


print("Number of partitions:", df.rdd.getNumPartitions())

df.write.mode("overwrite").parquet(
    "D:/capstone1/parquet_output/raw_parquet"
)








# Existing calculations
row_count = df.count()

df = df.withColumn("source_file", input_file_name())

source_file_count = df.select("source_file").distinct().count()

source_files = [
    row["source_file"]
    for row in df.select("source_file").distinct().collect()
]

unique_grids = df.select("CellID").distinct().count()

country_codes = df.select("countrycode").distinct().count()

hourly_intervals = df.select("datetime").distinct().count()

df = df.withColumn(
    "hour",
    date_trunc("hour", "datetime")
)

hourly_intervals_truncated = df.select("hour").distinct().count()

partition_count = df.rdd.getNumPartitions()

# Create summary dictionary
summary = {
    "file_count": len(actual_files),
    "files_found": [os.path.basename(f) for f in actual_files],
    "total_rows": row_count,
    "source_file_count": source_file_count,
    "source_files": source_files,
    "unique_cell_ids": unique_grids,
    "country_code_categories": country_codes,
    "distinct_datetime_intervals": hourly_intervals,
    "distinct_hour_intervals": hourly_intervals_truncated,
    "num_partitions": partition_count
}

# Write JSON file
output_json = "D:/capstone1/phase2/sp1_summary.json"

with open(output_json, "w") as f:
    json.dump(summary, f, indent=4)

print(f"Summary written to: {output_json}")
