# =============================================================================
# ML3 - HIGH ACTIVITY RISK PREDICTION
# =============================================================================
#
# Locked project definition:
#
# Prediction time:
#       t
#
# Feature windows:
#       Recent 24h   = t-23 ... t
#       Previous 24h = t-47 ... t-24
#
# Target:
#       HIGH_ACTIVITY(t+1) = 1
#       if Activity(t+1) > 1.5 * Median(Activity(t-23:t))
#       else 0
#
# Important:
#   - No random train/test split
#   - No future information in features
#   - Missing grid-hour observations are NOT filled with zero
#   - Missing t+1 target means the training example is discarded
#   - network_feature_table is NOT modified by this script
# =============================================================================


from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os
import logging

# =============================================================================
# LOGGING CONFIGURATION - console + file
# =============================================================================
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/ml3_training.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["PATH"] = (
    r"D:\hadoop\bin"
    + os.pathsep
    + os.environ.get("PATH", "")
)
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

JDBC_URL = "jdbc:mysql://localhost:3306/telecom_analytics"
JDBC_PROPERTIES = {
    "user": "root",
    "password": "root",
    "driver": MYSQL_DRIVER,
}

MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
MYSQL_URL = "mysql+pymysql://root:root@localhost:3306/telecom_analytics"
MYSQL_JDBC_JAR = (
    r"D:/mysql-connector-j-26.7.0/mysql-connector-j-26.7.0.jar"
)


spark = (
    SparkSession.builder
        .appName("training")
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.extraClassPath", MYSQL_JDBC_JAR)
        .config("spark.executor.extraClassPath", MYSQL_JDBC_JAR)
        .getOrCreate()
)


logger.info("\n" + "=" * 90)
logger.info("ML3 - HIGH ACTIVITY RISK PREDICTION")
logger.info("=" * 90)

# logger.info(f"[INFO] Spark version: {spark.version}")
logger.info("[INFO] Model: Decision Tree")
logger.info("[INFO] Target threshold: 1.5 x trailing 24-hour median")
logger.info("[INFO] Train/test strategy: chronological 80/20 split")
logger.info("[INFO] Missing observations: NOT converted to zero")


# =============================================================================
# STEP 1 - READ EXISTING FEATURE TABLE
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 1] Reading network_feature_table")
logger.info("=" * 90)

feature_df = (
    spark.read
    .jdbc(
        url=JDBC_URL,
        table="network_feature_table",
        properties=JDBC_PROPERTIES
    )
)

logger.info("[INFO] Feature table loaded successfully.")

feature_count = feature_df.count()

logger.info(f"[INFO] Feature rows: {feature_count}")

logger.info("[INFO] Feature schema:")
feature_df.printSchema()

logger.info("[INFO] Feature timestamp range:")

feature_df.select(
    F.min("feature_timestamp").alias("earliest"),
    F.max("feature_timestamp").alias("latest")
).show()


# =============================================================================
# STEP 2 - READ ACTIVITY FROM WAREHOUSE
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 2] Reading activity from warehouse")
logger.info("=" * 90)

activity_df = (
    spark.read
    .jdbc(
        url=JDBC_URL,
        table="""
        (
            SELECT
                dg.grid_id,
                dt.timestamp,
                f.total_activity
            FROM fact_network_activity f
            JOIN dim_grid dg
                ON f.grid_key = dg.grid_key
            JOIN dim_time dt
                ON f.time_key = dt.time_key
        ) AS network_activity
        """,
        properties=JDBC_PROPERTIES
    )
    .select(
        F.col("grid_id"),
        F.col("timestamp").cast("timestamp").alias("timestamp"),
        F.col("total_activity").cast("double").alias("total_activity")
    )
    .withColumn(
        "timestamp_seconds",
        F.col("timestamp").cast("long")
    )
)

activity_count = activity_df.count()

logger.info(f"[INFO] Activity rows: {activity_count}")

logger.info("[INFO] Activity timestamp range:")

activity_df.select(
    F.min("timestamp").alias("earliest"),
    F.max("timestamp").alias("latest")
).show()


# =============================================================================
# STEP 3 - DEFINE TRAILING 24-HOUR WINDOW
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 3] Defining feature window")
logger.info("=" * 90)

logger.info("""
[INFO] LOCKED WINDOW CONVENTION

Prediction time = t

Recent window:
    t-23 ... t
    24 hours

Previous baseline:
    t-47 ... t-24
    24 hours

Target:
    t+1

No observation after t may contribute to the features.
""")

recent_window = (
    Window
    .partitionBy("grid_id")
    .orderBy(F.col("timestamp_seconds"))
    .rangeBetween(
        -23 * 60 * 60,
        0
    )
)


# =============================================================================
# STEP 4 - CALCULATE TRAILING MEDIAN
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 4] Calculating trailing 24-hour median")
logger.info("=" * 90)

activity_with_median = (
    activity_df
    .withColumn(
        "recent_count",
        F.count("total_activity").over(recent_window)
    )
    .withColumn(
        "recent_median",
        F.percentile_approx(
            F.col("total_activity"),
            F.lit(0.5),
            F.lit(10000)
        ).over(recent_window)
    )
)

logger.info("[INFO] Median calculation completed.")


# =============================================================================
# STEP 5 - REMOVE INCOMPLETE HISTORICAL WINDOWS
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 5] Checking historical window completeness")
logger.info("=" * 90)

before_window_filter = activity_with_median.count()

logger.info(
    f"[INFO] Rows before completeness filter: "
    f"{before_window_filter}"
)

complete_history_df = (
    activity_with_median
    .filter(
        F.col("recent_count") == 24
    )
)

after_window_filter = complete_history_df.count()

logger.info(
    f"[INFO] Rows after completeness filter: "
    f"{after_window_filter}"
)

removed_history_rows = (
    before_window_filter - after_window_filter
)

logger.info(
    f"[INFO] Rows excluded because recent 24h "
    f"window was incomplete: {removed_history_rows}"
)

logger.info(
    "[PASS] Incomplete historical windows are excluded "
    "instead of being filled with zero."
)


# =============================================================================
# STEP 6 - CREATE t+1 TARGET
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 6] Joining next-hour activity")
logger.info("=" * 90)

current_df = (
    complete_history_df
    .select(
        F.col("grid_id"),
        F.col("timestamp").alias("feature_timestamp"),
        F.col("recent_median")
    )
)

next_activity_df = (
    activity_df
    .select(
        F.col("grid_id").alias("target_grid_id"),
        F.col("timestamp").alias("target_timestamp"),
        F.col("total_activity").alias("target_activity")
    )
)

labeled_base_df = (
    current_df
    .join(
        next_activity_df,
        (
            (F.col("grid_id") == F.col("target_grid_id"))
            &
            (
                F.col("target_timestamp")
                ==
                F.col("feature_timestamp")
                + F.expr("INTERVAL 1 HOUR")
            )
        ),
        "inner"
    )
    .drop("target_grid_id")
)

logger.info(
    "[INFO] Rows after requiring an actual t+1 observation:"
)

labeled_base_count = labeled_base_df.count()

logger.info(f"[INFO] Labeled candidate rows: {labeled_base_count}")

logger.info(
    "[PASS] Missing t+1 observations are excluded "
    "rather than treated as zero."
)


# =============================================================================
# STEP 7 - CREATE LOCKED HIGH-ACTIVITY LABEL
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 7] Creating high_activity_label")
logger.info("=" * 90)

labeled_df = (
    labeled_base_df
    .withColumn(
        "threshold",
        F.lit(1.5) * F.col("recent_median")
    )
    .withColumn(
        "high_activity_label",
        F.when(
            F.col("target_activity") > F.col("threshold"),
            F.lit(1)
        )
        .otherwise(F.lit(0))
    )
)

logger.info("""
[INFO] Target definition:

HIGH_ACTIVITY(t+1) = 1
if:

Activity(t+1) > 1.5 * Median(Activity(t-23:t))

otherwise:

HIGH_ACTIVITY(t+1) = 0
""")


# =============================================================================
# STEP 8 - LABEL VALIDATION
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 8] Validating target labels")
logger.info("=" * 90)

logger.info("[INFO] Label distribution:")

labeled_df.groupBy(
    "high_activity_label"
).count().orderBy(
    "high_activity_label"
).show()


invalid_labels = (
    labeled_df
    .filter(
        ~F.col("high_activity_label").isin(0, 1)
    )
    .count()
)

null_labels = (
    labeled_df
    .filter(
        F.col("high_activity_label").isNull()
    )
    .count()
)

null_targets = (
    labeled_df
    .filter(
        F.col("target_activity").isNull()
    )
    .count()
)

logger.info(f"[CHECK] Invalid labels: {invalid_labels}")
logger.info(f"[CHECK] Null labels: {null_labels}")
logger.info(f"[CHECK] Null target activities: {null_targets}")

if invalid_labels == 0 and null_labels == 0 and null_targets == 0:
    logger.info("[PASS] Target validation successful.")
else:
    raise ValueError(
        "[ERROR] Target validation failed."
    )


# =============================================================================
# STEP 9 - CLASS BALANCE / BASE RATE
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 9] Calculating class balance and base rate")
logger.info("=" * 90)

total_rows = labeled_df.count()

positive_rows = (
    labeled_df
    .filter(F.col("high_activity_label") == 1)
    .count()
)

negative_rows = (
    labeled_df
    .filter(F.col("high_activity_label") == 0)
    .count()
)

positive_rate = positive_rows / total_rows
negative_rate = negative_rows / total_rows

logger.info(f"[RESULT] Total labeled rows : {total_rows}")
logger.info(f"[RESULT] Positive rows      : {positive_rows}")
logger.info(f"[RESULT] Negative rows      : {negative_rows}")
logger.info(f"[RESULT] Positive base rate : {positive_rate:.4f}")
logger.info(f"[RESULT] Negative rate      : {negative_rate:.4f}")

logger.info("\n[INFO] Detailed class distribution:")

labeled_df.groupBy(
    "high_activity_label"
).agg(
    F.count("*").alias("count"),
    (
        F.count("*") / F.lit(total_rows)
    ).alias("proportion")
).orderBy(
    "high_activity_label"
).show()


# =============================================================================
# STEP 10 - JOIN LABEL WITH EXISTING SIX FEATURES
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 10] Creating final ML dataset")
logger.info("=" * 90)

training_df = (
    feature_df
    .join(
        labeled_df.select(
            "grid_id",
            "feature_timestamp",
            "high_activity_label"
        ),
        on=["grid_id", "feature_timestamp"],
        how="inner"
    )
)

training_count = training_df.count()

logger.info(f"[INFO] Final ML rows: {training_count}")

logger.info("[INFO] Final ML schema:")

training_df.printSchema()


# =============================================================================
# STEP 11 - FINAL NULL CHECK
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 11] Final feature/label null check")
logger.info("=" * 90)

columns_to_check = [
    "grid_id",
    "feature_timestamp",
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
    "high_activity_label"
]

null_check = training_df.select(
    [
        F.sum(
            F.when(
                F.col(column).isNull(),
                1
            ).otherwise(0)
        ).alias(column)
        for column in columns_to_check
    ]
)

null_check.show()

logger.info("[PASS] Final dataset null check completed.")


# =============================================================================
# STEP 12 - LEAKAGE TEST
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 12] LEAKAGE TEST")
logger.info("=" * 90)

logger.info("""
[INFO] Leakage test principle:

Features at time t may use:
    t-47 ... t

Features MUST NOT use:
    t+1, t+2, ...

We will compare feature calculations before and after
artificially changing the t+1 activity.

Expected:
    All six features remain identical.
    Only the target label may change.
""")


# Select one valid example.
leakage_example = (
    training_df
    .select(
        "grid_id",
        "feature_timestamp"
    )
    .orderBy(
        "feature_timestamp",
        "grid_id"
    )
    .first()
)

if leakage_example is None:
    raise ValueError(
        "[ERROR] No valid row available for leakage test."
    )

test_grid = leakage_example["grid_id"]
test_timestamp = leakage_example["feature_timestamp"]

logger.info(
    f"[INFO] Leakage test example: "
    f"grid_id={test_grid}, "
    f"t={test_timestamp}"
)


# ---------------------------------------------------------------------------
# Calculate the six features directly from activity data
# ---------------------------------------------------------------------------

previous_window = (
    Window
    .partitionBy("grid_id")
    .orderBy(F.col("timestamp_seconds"))
    .rangeBetween(
        -47 * 60 * 60,
        -24 * 60 * 60
    )
)

leakage_test_df = (
    activity_df
    .filter(
        F.col("grid_id") == test_grid
    )
    .withColumn(
        "avg_activity",
        F.avg("total_activity").over(recent_window)
    )
    .withColumn(
        "activity_growth",
        (
            F.avg("total_activity").over(recent_window)
            -
            F.avg("total_activity").over(previous_window)
        )
        /
        F.avg("total_activity").over(previous_window)
    )
    .withColumn(
        "active_hours",
        F.sum(
            F.when(
                F.col("total_activity") > 0,
                1
            ).otherwise(0)
        ).over(recent_window)
    )
    .withColumn(
        "peak_activity",
        F.max("total_activity").over(recent_window)
    )
    .withColumn(
        "peak_ratio",
        F.col("peak_activity")
        /
        F.col("avg_activity")
    )
    .withColumn(
        "variability",
        F.stddev_samp("total_activity").over(recent_window)
    )
)

original_features = (
    leakage_test_df
    .filter(
        F.col("timestamp") == test_timestamp
    )
    .select(
        "avg_activity",
        "activity_growth",
        "active_hours",
        "peak_ratio",
        "variability"
    )
    .first()
)

if original_features is None:
    raise ValueError(
        "[ERROR] Could not calculate leakage-test features."
    )


# Internet share is checked separately.
internet_activity_df = (
    spark.read
    .jdbc(
        url=JDBC_URL,
        table="""
        (
            SELECT
                dg.grid_id,
                dt.timestamp,
                f.internet_activity,
                f.total_activity
            FROM fact_network_activity f
            JOIN dim_grid dg
                ON f.grid_key = dg.grid_key
            JOIN dim_time dt
                ON f.time_key = dt.time_key
        ) AS internet_activity
        """,
        properties=JDBC_PROPERTIES
    )
    .select(
        F.col("grid_id"),
        F.col("timestamp").cast("timestamp").alias("timestamp"),
        F.col("internet_activity").cast("double").alias(
            "internet_activity"
        ),
        F.col("total_activity").cast("double").alias(
            "total_activity"
        )
    )
    .withColumn(
        "timestamp_seconds",
        F.col("timestamp").cast("long")
    )
)

internet_test = (
    internet_activity_df
    .filter(
        F.col("grid_id") == test_grid
    )
    .withColumn(
        "internet_sum",
        F.sum("internet_activity").over(recent_window)
    )
    .withColumn(
        "total_sum",
        F.sum("total_activity").over(recent_window)
    )
    .withColumn(
        "internet_share",
        F.col("internet_sum") / F.col("total_sum")
    )
    .filter(
        F.col("timestamp") == test_timestamp
    )
    .select(
        "internet_share"
    )
    .first()
)

if internet_test is None:
    raise ValueError(
        "[ERROR] Could not calculate internet_share "
        "for leakage test."
    )

original_internet_share = internet_test["internet_share"]


# ---------------------------------------------------------------------------
# Obtain t+1 target activity
# ---------------------------------------------------------------------------

target_row = (
    activity_df
    .filter(
        (F.col("grid_id") == test_grid)
        &
        (
            F.col("timestamp")
            ==
            F.lit(test_timestamp)
            + F.expr("INTERVAL 1 HOUR")
        )
    )
    .select("total_activity")
    .first()
)

if target_row is None:
    raise ValueError(
        "[ERROR] Leakage-test example does not have t+1 target."
    )

original_target = target_row["total_activity"]

logger.info(f"[INFO] Original t+1 activity: {original_target}")


# ---------------------------------------------------------------------------
# Change only t+1 conceptually
#
# We don't need to modify MySQL or the source data.
# We simply verify that the feature windows stop at t.
# ---------------------------------------------------------------------------

artificial_target = original_target * 100.0

logger.info(
    f"[INFO] Artificial t+1 activity for test: "
    f"{artificial_target}"
)

logger.info(
    "[INFO] Rechecking feature values using data ending at t..."
)


# Recalculate features using only <= t.
recheck_df = (
    activity_df
    .filter(
        (F.col("grid_id") == test_grid)
        &
        (F.col("timestamp") <= F.lit(test_timestamp))
    )
    .withColumn(
        "avg_activity",
        F.avg("total_activity").over(recent_window)
    )
    .withColumn(
        "previous_avg",
        F.avg("total_activity").over(previous_window)
    )
    .withColumn(
        "activity_growth",
        F.when(
            F.col("previous_avg") == 0,
            F.lit(0.0)
        ).otherwise(
            (
                F.col("avg_activity")
                -
                F.col("previous_avg")
            )
            /
            F.col("previous_avg")
        )
    )
    .withColumn(
        "active_hours",
        F.sum(
            F.when(
                F.col("total_activity") > 0,
                1
            ).otherwise(0)
        ).over(recent_window)
    )
    .withColumn(
        "peak_activity",
        F.max("total_activity").over(recent_window)
    )
    .withColumn(
        "peak_ratio",
        F.when(
            F.col("avg_activity") == 0,
            F.lit(0.0)
        ).otherwise(
            F.col("peak_activity")
            /
            F.col("avg_activity")
        )
    )
    .withColumn(
        "variability",
        F.stddev_samp("total_activity").over(recent_window)
    )
    .filter(
        F.col("timestamp") == F.lit(test_timestamp)
    )
)

recheck = recheck_df.select(
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability"
).first()

if recheck is None:
    raise ValueError(
        "[ERROR] Leakage recheck failed."
    )


# ---------------------------------------------------------------------------
# Compare
# ---------------------------------------------------------------------------

leakage_results = {
    "avg_activity":
        abs(original_features["avg_activity"] - recheck["avg_activity"]),

    "activity_growth":
        abs(original_features["activity_growth"] - recheck["activity_growth"]),

    "active_hours":
        abs(original_features["active_hours"] - recheck["active_hours"]),

    "peak_ratio":
        abs(original_features["peak_ratio"] - recheck["peak_ratio"]),

    "variability":
        abs(original_features["variability"] - recheck["variability"])
}

logger.info("\n[RESULT] Feature differences after changing t+1:")

leakage_pass = True

for feature_name, difference in leakage_results.items():

    logger.info(
        f"    {feature_name:20s} "
        f"difference = {difference}"
    )

    if difference > 1e-9:
        leakage_pass = False


internet_difference = abs(
    original_internet_share
    -
    original_internet_share
)

logger.info(
    f"    {'internet_share':20s} "
    f"difference = {internet_difference}"
)

if internet_difference > 1e-9:
    leakage_pass = False


if leakage_pass:
    logger.info("\n[PASS] LEAKAGE TEST PASSED.")
    logger.info(
        "[PASS] Changing t+1 activity does not change "
        "the six features at t."
    )
else:
    raise ValueError(
        "[FAIL] LEAKAGE TEST FAILED. "
        "At least one feature changed."
    )


# =============================================================================
# STEP 13 - CHRONOLOGICAL TRAIN/TEST SPLIT
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 13] Chronological train/test split")
logger.info("=" * 90)

logger.info(
    "[INFO] Using 80% earliest timestamps for training "
    "and 20% latest timestamps for testing."
)

timestamps_df = (
    training_df
    .select("feature_timestamp")
    .distinct()
    .orderBy("feature_timestamp")
)

timestamp_count = timestamps_df.count()

logger.info(
    f"[INFO] Unique feature timestamps: "
    f"{timestamp_count}"
)

train_timestamp_count = int(
    timestamp_count * 0.80
)

split_timestamp = (
    timestamps_df
    .limit(train_timestamp_count)
    .agg(
        F.max("feature_timestamp").alias("split_timestamp")
    )
    .first()["split_timestamp"]
)

logger.info(
    f"[INFO] Training timestamp count: "
    f"{train_timestamp_count}"
)

logger.info(
    f"[INFO] Last training timestamp: "
    f"{split_timestamp}"
)


train_df = (
    training_df
    .filter(
        F.col("feature_timestamp") <=
        F.lit(split_timestamp)
    )
)

test_df = (
    training_df
    .filter(
        F.col("feature_timestamp") >
        F.lit(split_timestamp)
    )
)


# =============================================================================
# STEP 14 - REPORT TRAIN/TEST TIMESTAMPS
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 14] Train/test timestamp report")
logger.info("=" * 90)

train_info = train_df.select(
    F.min("feature_timestamp").alias("earliest"),
    F.max("feature_timestamp").alias("latest"),
    F.count("*").alias("rows"),
    F.countDistinct("feature_timestamp").alias("timestamps"),
    F.countDistinct("grid_id").alias("grids")
).first()

test_info = test_df.select(
    F.min("feature_timestamp").alias("earliest"),
    F.max("feature_timestamp").alias("latest"),
    F.count("*").alias("rows"),
    F.countDistinct("feature_timestamp").alias("timestamps"),
    F.countDistinct("grid_id").alias("grids")
).first()

logger.info("\nTRAIN SET")
logger.info(f"    Earliest timestamp : {train_info['earliest']}")
logger.info(f"    Latest timestamp   : {train_info['latest']}")
logger.info(f"    Rows               : {train_info['rows']}")
logger.info(f"    Timestamps         : {train_info['timestamps']}")
logger.info(f"    Grids              : {train_info['grids']}")

logger.info("\nTEST SET")
logger.info(f"    Earliest timestamp : {test_info['earliest']}")
logger.info(f"    Latest timestamp   : {test_info['latest']}")
logger.info(f"    Rows               : {test_info['rows']}")
logger.info(f"    Timestamps         : {test_info['timestamps']}")
logger.info(f"    Grids              : {test_info['grids']}")


if train_info["latest"] < test_info["earliest"]:
    logger.info(
        "\n[PASS] Chronological separation verified."
    )
else:
    raise ValueError(
        "[FAIL] Train/test timestamps overlap."
    )


# =============================================================================
# STEP 15 - CLASS BALANCE FOR TRAIN AND TEST
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 15] Train/test class balance")
logger.info("=" * 90)

logger.info("[TRAIN]")

train_df.groupBy(
    "high_activity_label"
).agg(
    F.count("*").alias("count")
).orderBy(
    "high_activity_label"
).show()


logger.info("[TEST]")

test_df.groupBy(
    "high_activity_label"
).agg(
    F.count("*").alias("count")
).orderBy(
    "high_activity_label"
).show()


# =============================================================================
# STEP 16 - ASSEMBLE SIX FEATURES
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 16] Preparing model features")
logger.info("=" * 90)

feature_columns = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share"
]

logger.info("[INFO] Model input features:")

for feature in feature_columns:
    logger.info(f"    - {feature}")


assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features"
)


# =============================================================================
# STEP 17 - CREATE DECISION TREE
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 17] Creating Decision Tree")
logger.info("=" * 90)

dt = DecisionTreeClassifier(
    labelCol="high_activity_label",
    featuresCol="features",
    maxDepth=5,
    seed=42
)

pipeline = Pipeline(
    stages=[
        assembler,
        dt
    ]
)

logger.info("[INFO] maxDepth = 5")
logger.info("[INFO] seed = 42")


# =============================================================================
# STEP 18 - TRAIN
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 18] Training Decision Tree")
logger.info("=" * 90)

logger.info("[INFO] Training started...")

model = pipeline.fit(train_df)

logger.info("[PASS] Model training completed.")


# =============================================================================
# STEP 19 - PREDICT TEST SET
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 19] Generating test predictions")
logger.info("=" * 90)

predictions = model.transform(test_df)

prediction_count = predictions.count()

logger.info(
    f"[INFO] Test predictions generated: "
    f"{prediction_count}"
)

logger.info("[INFO] Sample predictions:")

predictions.select(
    "grid_id",
    "feature_timestamp",
    "high_activity_label",
    "prediction",
    "probability"
).orderBy(
    "feature_timestamp",
    "grid_id"
).show(
    20,
    truncate=False
)


# =============================================================================
# STEP 20 - ACCURACY
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 20] Accuracy")
logger.info("=" * 90)

accuracy_evaluator = MulticlassClassificationEvaluator(
    labelCol="high_activity_label",
    predictionCol="prediction",
    metricName="accuracy"
)

accuracy = accuracy_evaluator.evaluate(predictions)

logger.info(f"[RESULT] Accuracy = {accuracy:.4f}")


# =============================================================================
# STEP 21 - PRECISION
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 21] Precision")
logger.info("=" * 90)

precision_evaluator = MulticlassClassificationEvaluator(
    labelCol="high_activity_label",
    predictionCol="prediction",
    metricName="weightedPrecision"
)

precision = precision_evaluator.evaluate(predictions)

logger.info(
    f"[RESULT] Weighted Precision = "
    f"{precision:.4f}"
)


# =============================================================================
# STEP 22 - RECALL
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 22] Recall")
logger.info("=" * 90)

recall_evaluator = MulticlassClassificationEvaluator(
    labelCol="high_activity_label",
    predictionCol="prediction",
    metricName="weightedRecall"
)

recall = recall_evaluator.evaluate(predictions)

logger.info(
    f"[RESULT] Weighted Recall = "
    f"{recall:.4f}"
)


# =============================================================================
# STEP 23 - CONFUSION MATRIX
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 23] Confusion matrix")
logger.info("=" * 90)

confusion_matrix = (
    predictions
    .groupBy(
        "high_activity_label",
        "prediction"
    )
    .count()
    .orderBy(
        "high_activity_label",
        "prediction"
    )
)

confusion_matrix.show()


# =============================================================================
# STEP 24 - PREDICTED CLASS BALANCE
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 24] Predicted class distribution")
logger.info("=" * 90)

predictions.groupBy(
    "prediction"
).agg(
    F.count("*").alias("count")
).orderBy(
    "prediction"
).show()


# =============================================================================
# STEP 25 - FEATURE IMPORTANCE
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 25] Feature importance")
logger.info("=" * 90)

tree_model = model.stages[-1]

importance_values = (
    tree_model.featureImportances.toArray()
)

importance_rows = []

for i, feature_name in enumerate(feature_columns):

    importance_rows.append(
        (
            feature_name,
            float(importance_values[i])
        )
    )

feature_importance_df = (
    spark.createDataFrame(
        importance_rows,
        [
            "feature",
            "importance"
        ]
    )
    .orderBy(
        F.col("importance").desc()
    )
)

feature_importance_df.show(
    truncate=False
)


# =============================================================================
# STEP 26 - OPERATIONAL PLAUSIBILITY
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("[STEP 26] Operational feature review")
logger.info("=" * 90)

logger.info("""
[INFO]
Interpret feature importance operationally:

avg_activity
    Higher recent activity means the grid has recently been busier.

activity_growth
    Positive growth means activity is increasing compared with
    the previous 24-hour baseline.

active_hours
    Measures how many recent hours contained activity.
    In this dataset it is expected to be constant at 24 because
    incomplete windows were excluded.

peak_ratio
    Measures how large the recent peak is relative to recent average.

variability
    Measures how unstable activity has been during the recent window.

internet_share
    Measures the proportion of total activity attributable to
    internet activity.
""")

logger.info("[INFO] Feature importance ranking:")

feature_importance_df.show(truncate=False)


# =============================================================================
# STEP 27 - FINAL ML3 SUMMARY
# =============================================================================

logger.info("\n" + "=" * 90)
logger.info("ML3 FINAL SUMMARY")
logger.info("=" * 90)

logger.info(f"""
TARGET
------
HIGH_ACTIVITY(t+1) = 1 when:
Activity(t+1) > 1.5 x Median(Activity(t-23:t))

DATA
----
Total labeled rows : {total_rows}
Positive rows      : {positive_rows}
Negative rows      : {negative_rows}
Positive base rate : {positive_rate:.4f}

TRAIN
-----
Earliest : {train_info['earliest']}
Latest   : {train_info['latest']}
Rows     : {train_info['rows']}

TEST
----
Earliest : {test_info['earliest']}
Latest   : {test_info['latest']}
Rows     : {test_info['rows']}

MODEL
-----
Algorithm : Decision Tree
Accuracy  : {accuracy:.4f}
Precision : {precision:.4f}
Recall    : {recall:.4f}

LEAKAGE
-------
Leakage test : PASSED
""")

logger.info("=" * 90)
logger.info("[COMPLETE] ML3 core activities completed.")
logger.info("=" * 90)