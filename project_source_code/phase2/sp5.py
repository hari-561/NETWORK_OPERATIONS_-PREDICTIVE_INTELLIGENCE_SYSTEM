# ============================================================
# PHASE 2 - SP5
# Spark Performance and Execution Analysis
# ============================================================

import logging
from pathlib import Path
from time import perf_counter

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
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

log_directory = Path("D:/capstone1/log")

log_directory.mkdir(
    parents=True,
    exist_ok=True
)

log_file = log_directory / "sp5.log"


logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("SP5")


logger.info("=" * 60)
logger.info("SP5 START")
logger.info("=" * 60)


print("\n" + "=" * 60)
print("                 SP5 START")
print("=" * 60)


# ============================================================
# 2. INPUT DATA
# ============================================================
#
# Expected DataFrames from previous stages:
#
# hourly_grid_summary -> SP3
# grid_lookup         -> SP4
# grid_activity_geo_df -> SP4
#
# We will use hourly_grid_summary for most performance tests.
# ============================================================

hourly_grid_summary = spark.read.parquet(
    "D:/capstone1/parquet_output/hourly_grid_parquet"
)

grid_lookup = spark.read.parquet(
    "D:/capstone1/parquet_output/grid_lookup_parquet"
)


grid_activity_geo_df = spark.read.parquet(
    "D:/capstone1/parquet_output/grid_activity_geo_parquet"
)




logger.info(
    "Required SP3/SP4 DataFrames are available"
)


# ============================================================
# 3. BASIC INPUT INFORMATION
# ============================================================

input_partitions = (
    hourly_grid_summary
    .rdd
    .getNumPartitions()
)

input_count = hourly_grid_summary.count()


print("\n========== INPUT INFORMATION ==========")

print(
    "Hourly grid rows:",
    input_count
)

print(
    "Initial partitions:",
    input_partitions
)


logger.info(
    "Hourly grid rows: %d",
    input_count
)

logger.info(
    "Initial hourly_grid_summary partitions: %d",
    input_partitions
)


# ============================================================
# 4. EXPLAIN HOTSPOT AGGREGATION
#
# Requirement:
# Run explain() on a hotspot aggregation and read
# the physical plan.
# ============================================================

print(
    "\n========== HOTSPOT AGGREGATION PLAN =========="
)


hotspot_aggregation = (
    hourly_grid_summary
    .groupBy("grid_id")
    .agg(
        F.sum(
            "total_activity"
        ).alias(
            "total_grid_activity"
        )
    )
    .orderBy(
        F.desc(
            "total_grid_activity"
        )
    )
    .limit(10)
)


hotspot_aggregation.explain(
    mode="formatted"
)


logger.info(
    "Hotspot aggregation physical plan inspected"
)


# ============================================================
# 5. EXPLAIN PLAN INTERPRETATION
# ============================================================
#
# We cannot programmatically rely on exact operator names
# across Spark versions, so the log records what we expect
# to observe.
# ============================================================

logger.info(
    "Hotspot aggregation is expected to contain "
    "a partial aggregation followed by a shuffle/exchange "
    "and final aggregation because grid_id is the grouping key"
)

logger.info(
    "The orderBy on aggregated activity introduces "
    "a sorting stage for ranking the top grids"
)


# ============================================================
# 6. PERFORMANCE TEST HELPER
# ============================================================

def time_action(
    dataframe,
    action_name
):
    """
    Execute a Spark action and return:
        result
        elapsed_seconds
    """

    start_time = perf_counter()

    result = dataframe.count()

    end_time = perf_counter()

    elapsed = (
        end_time - start_time
    )

    logger.info(
        "%s: %.4f seconds",
        action_name,
        elapsed
    )

    return result, elapsed


# ============================================================
# 7. CACHE EXPERIMENT
#
# First demonstrate repeated actions WITHOUT caching.
# ============================================================

print(
    "\n========== CACHE EXPERIMENT =========="
)


uncached_df = hourly_grid_summary


uncached_count_1, uncached_time_1 = (
    time_action(
        uncached_df,
        "Uncached action 1"
    )
)


uncached_count_2, uncached_time_2 = (
    time_action(
        uncached_df,
        "Uncached action 2"
    )
)


print(
    f"Uncached action 1: "
    f"{uncached_time_1:.4f} seconds"
)

print(
    f"Uncached action 2: "
    f"{uncached_time_2:.4f} seconds"
)


logger.info(
    "Uncached repeated action timings: "
    "action1=%.4f sec, action2=%.4f sec",
    uncached_time_1,
    uncached_time_2
)


# ============================================================
# 8. CACHE REUSED DATAFRAME
# ============================================================

cached_df = (
    hourly_grid_summary
    .cache()
)


# Materialize cache.

cache_materialization_start = perf_counter()

cached_count = cached_df.count()

cache_materialization_end = perf_counter()

cache_materialization_time = (
    cache_materialization_end
    - cache_materialization_start
)


logger.info(
    "Cache materialization time: %.4f seconds",
    cache_materialization_time
)


# ============================================================
# 9. TIME REPEATED CACHED ACTIONS
# ============================================================

cached_count_1, cached_time_1 = (
    time_action(
        cached_df,
        "Cached action 1"
    )
)


cached_count_2, cached_time_2 = (
    time_action(
        cached_df,
        "Cached action 2"
    )
)


print(
    f"Cache materialization: "
    f"{cache_materialization_time:.4f} seconds"
)

print(
    f"Cached action 1: "
    f"{cached_time_1:.4f} seconds"
)

print(
    f"Cached action 2: "
    f"{cached_time_2:.4f} seconds"
)


logger.info(
    "Cached repeated action timings: "
    "action1=%.4f sec, action2=%.4f sec",
    cached_time_1,
    cached_time_2
)


# ============================================================
# 10. CACHE VALIDATION
# ============================================================

assert (
    uncached_count_1
    == cached_count
    == cached_count_1
    == cached_count_2
)


logger.info(
    "Cache correctness check: PASS"
)


# ============================================================
# 11. CACHE PERFORMANCE OBSERVATION
# ============================================================

uncached_average = (
    uncached_time_1
    + uncached_time_2
) / 2


cached_average = (
    cached_time_1
    + cached_time_2
) / 2


if uncached_average > 0:

    cache_speedup = (
        uncached_average
        / cached_average
    )

else:

    cache_speedup = 0.0


print(
    f"Average uncached time: "
    f"{uncached_average:.4f} seconds"
)

print(
    f"Average cached time: "
    f"{cached_average:.4f} seconds"
)

print(
    f"Approximate cache speedup: "
    f"{cache_speedup:.2f}x"
)


logger.info(
    "Average uncached action time: %.4f seconds",
    uncached_average
)

logger.info(
    "Average cached action time: %.4f seconds",
    cached_average
)

logger.info(
    "Approximate cache speedup: %.2fx",
    cache_speedup
)


# ============================================================
# 12. REPARTITIONING EXPERIMENT
#
# Repartition by date.
# ============================================================

print(
    "\n========== REPARTITIONING EXPERIMENT =========="
)


before_repartition = (
    cached_df
    .rdd
    .getNumPartitions()
)


repartitioned_df = (
    cached_df
    .repartition(
        "date"
    )
)


after_repartition = (
    repartitioned_df
    .rdd
    .getNumPartitions()
)


print(
    "Partitions before repartition:",
    before_repartition
)

print(
    "Partitions after repartition:",
    after_repartition
)


logger.info(
    "Partitions before repartition by date: %d",
    before_repartition
)

logger.info(
    "Partitions after repartition by date: %d",
    after_repartition
)


# ============================================================
# 13. MATERIALIZE REPARTITIONED DATA
# ============================================================

repartition_start = perf_counter()

repartitioned_count = (
    repartitioned_df.count()
)

repartition_end = perf_counter()

repartition_time = (
    repartition_end
    - repartition_start
)


print(
    "Repartitioned row count:",
    repartitioned_count
)

print(
    f"Repartition action time: "
    f"{repartition_time:.4f} seconds"
)


logger.info(
    "Repartitioned row count: %d",
    repartitioned_count
)

logger.info(
    "Repartition action time: %.4f seconds",
    repartition_time
)


assert (
    repartitioned_count
    == input_count
)


logger.info(
    "Repartition row preservation check: PASS"
)


# ============================================================
# 14. PARTITION DISTRIBUTION
# ============================================================

partition_sizes = (
    repartitioned_df
    .rdd
    .mapPartitions(
        lambda iterator: [sum(1 for _ in iterator)]
    )
    .collect()
)


non_empty_partitions = sum(
    1
    for size in partition_sizes
    if size > 0
)


empty_partitions = sum(
    1
    for size in partition_sizes
    if size == 0
)


largest_partition = (
    max(partition_sizes)
    if partition_sizes
    else 0
)


smallest_non_empty_partition = (
    min(
        size
        for size in partition_sizes
        if size > 0
    )
    if non_empty_partitions > 0
    else 0
)


print(
    "Non-empty partitions:",
    non_empty_partitions
)

print(
    "Empty partitions:",
    empty_partitions
)

print(
    "Largest partition:",
    largest_partition
)

print(
    "Smallest non-empty partition:",
    smallest_non_empty_partition
)


logger.info(
    "Repartition distribution: "
    "non_empty=%d, empty=%d, largest=%d, "
    "smallest_non_empty=%d",
    non_empty_partitions,
    empty_partitions,
    largest_partition,
    smallest_non_empty_partition
)


# ============================================================
# 15. COLUMN PRUNING EXPERIMENT
#
# Aggregation requires only:
#
# grid_id
# total_activity
#
# Demonstrate selecting only these columns first.
# ============================================================

print(
    "\n========== COLUMN PRUNING =========="
)


full_columns = (
    len(cached_df.columns)
)


pruned_df = (
    cached_df
    .select(
        "grid_id",
        "total_activity"
    )
)


pruned_columns = (
    len(pruned_df.columns)
)


print(
    "Columns before pruning:",
    full_columns
)

print(
    "Columns after pruning:",
    pruned_columns
)

print(
    "Pruned columns:",
    pruned_df.columns
)


logger.info(
    "Column pruning demonstration: "
    "before=%d columns, after=%d columns",
    full_columns,
    pruned_columns
)


# ============================================================
# 16. PRUNED HOTSPOT AGGREGATION
# ============================================================

pruned_hotspot = (
    pruned_df
    .groupBy(
        "grid_id"
    )
    .agg(
        F.sum(
            "total_activity"
        ).alias(
            "total_grid_activity"
        )
    )
    .orderBy(
        F.desc(
            "total_grid_activity"
        )
    )
    .limit(10)
)


print(
    "\n========== PRUNED HOTSPOT PLAN =========="
)


pruned_hotspot.explain(
    mode="formatted"
)


logger.info(
    "Column-pruned hotspot aggregation plan inspected"
)


# ============================================================
# 17. STANDARD JOIN PLAN
#
# Use grid_lookup from SP4.
# ============================================================

print(
    "\n========== STANDARD GRID JOIN PLAN =========="
)


standard_join = (
    cached_df
    .join(
        grid_lookup,
        on="grid_id",
        how="left"
    )
)


standard_join.explain(
    mode="formatted"
)


logger.info(
    "Standard grid lookup join plan inspected"
)


# ============================================================
# 18. BROADCAST JOIN PLAN
# ============================================================

print(
    "\n========== BROADCAST GRID JOIN PLAN =========="
)


broadcast_join = (
    cached_df
    .join(
        broadcast(
            grid_lookup
        ),
        on="grid_id",
        how="left"
    )
)


broadcast_join.explain(
    mode="formatted"
)


logger.info(
    "Broadcast grid lookup join plan inspected"
)


# ============================================================
# 19. COMPARE JOIN DATASET SIZES
# ============================================================

grid_lookup_count = (
    grid_lookup.count()
)


activity_count = (
    cached_df.count()
)


print(
    "\nGrid lookup rows:",
    grid_lookup_count
)

print(
    "Activity rows:",
    activity_count
)


logger.info(
    "Grid lookup size: %d rows",
    grid_lookup_count
)

logger.info(
    "Activity dataset size: %d rows",
    activity_count
)


# ============================================================
# 20. BROADCAST CANDIDATE OBSERVATION
# ============================================================

if (
    grid_lookup_count
    < activity_count
):

    logger.info(
        "Broadcast candidate evidence: grid lookup "
        "is substantially smaller than activity dataset"
    )

else:

    logger.warning(
        "Grid lookup is not smaller than activity dataset"
    )


# ============================================================
# 21. OPTIONAL JOIN TIMING
#
# We trigger the joins with count().
# This provides practical evidence in addition
# to explain().
# ============================================================

print(
    "\n========== JOIN TIMING =========="
)


standard_join_start = perf_counter()

standard_join_count = (
    standard_join.count()
)

standard_join_end = perf_counter()

standard_join_time = (
    standard_join_end
    - standard_join_start
)


broadcast_join_start = perf_counter()

broadcast_join_count = (
    broadcast_join.count()
)

broadcast_join_end = perf_counter()

broadcast_join_time = (
    broadcast_join_end
    - broadcast_join_start
)


print(
    f"Standard join time: "
    f"{standard_join_time:.4f} seconds"
)

print(
    f"Broadcast join time: "
    f"{broadcast_join_time:.4f} seconds"
)


logger.info(
    "Standard join timing: %.4f seconds",
    standard_join_time
)

logger.info(
    "Broadcast join timing: %.4f seconds",
    broadcast_join_time
)


assert (
    standard_join_count
    == broadcast_join_count
)


logger.info(
    "Standard vs broadcast join row-count check: PASS"
)


# ============================================================
# 22. OVER-PARTITIONING DEMONSTRATION
#
# Demonstrate why excessive partitions can hurt a
# small/local dataset.
#
# We deliberately use a high partition count.
# ============================================================

print(
    "\n========== OVER-PARTITIONING EXPERIMENT =========="
)


current_partitions = (
    cached_df
    .rdd
    .getNumPartitions()
)


# Use a deliberately high number relative to the
# default/local partition count.

over_partition_count = max(
    current_partitions * 10,
    100
)


logger.info(
    "Current partitions: %d",
    current_partitions
)

logger.info(
    "Over-partitioning experiment target: %d",
    over_partition_count
)


over_partitioned_df = (
    cached_df
    .repartition(
        over_partition_count
    )
)


actual_over_partitions = (
    over_partitioned_df
    .rdd
    .getNumPartitions()
)


print(
    "Normal partitions:",
    current_partitions
)

print(
    "Over-partitioned partitions:",
    actual_over_partitions
)


logger.info(
    "Actual over-partitioned partition count: %d",
    actual_over_partitions
)


# ============================================================
# 23. TIME OVER-PARTITIONED ACTION
# ============================================================

over_partition_start = perf_counter()

over_partition_rows = (
    over_partitioned_df.count()
)

over_partition_end = perf_counter()

over_partition_time = (
    over_partition_end
    - over_partition_start
)


print(
    f"Over-partitioned action time: "
    f"{over_partition_time:.4f} seconds"
)


logger.info(
    "Over-partitioned action time: %.4f seconds",
    over_partition_time
)


assert (
    over_partition_rows
    == input_count
)


logger.info(
    "Over-partitioned row preservation check: PASS"
)


# ============================================================
# 24. PERFORMANCE OBSERVATION 1
#     CACHE
# ============================================================

if cached_average < uncached_average:

    cache_observation = (
        "Caching improved repeated-action performance. "
        f"Average uncached time was "
        f"{uncached_average:.4f}s versus "
        f"{cached_average:.4f}s after caching."
    )

else:

    cache_observation = (
        "Caching did not improve the measured repeated "
        "action time in this run. This can occur because "
        "the dataset is local, already materialized, or "
        "the cache/materialization overhead dominates."
    )


logger.info(
    "PERFORMANCE OBSERVATION 1 - CACHE: %s",
    cache_observation
)


# ============================================================
# 25. PERFORMANCE OBSERVATION 2
#     REPARTITIONING
# ============================================================

repartition_observation = (
    "Repartitioning by date changed the partition count "
    f"from {before_repartition} to "
    f"{after_repartition}. The repartition action took "
    f"{repartition_time:.4f}s and preserved all "
    f"{repartitioned_count} rows. The evidence shows "
    "that repartitioning introduces an execution cost and "
    "should be used when the resulting partition layout "
    "benefits downstream operations."
)


logger.info(
    "PERFORMANCE OBSERVATION 2 - REPARTITIONING: %s",
    repartition_observation
)


# ============================================================
# 26. PERFORMANCE OBSERVATION 3
#     BROADCAST JOIN
# ============================================================

if broadcast_join_time < standard_join_time:

    broadcast_observation = (
        "Broadcast join was faster than the standard join "
        f"in this run: {broadcast_join_time:.4f}s versus "
        f"{standard_join_time:.4f}s. The grid lookup contains "
        f"{grid_lookup_count} rows compared with "
        f"{activity_count} activity rows, making it a strong "
        "broadcast candidate."
    )

else:

    broadcast_observation = (
        "Broadcast join did not outperform the standard join "
        "in this run. However, the grid lookup remains a "
        f"broadcast candidate because it contains only "
        f"{grid_lookup_count} rows compared with "
        f"{activity_count} activity rows. "
        "The execution plan should be checked for the "
        "BroadcastHashJoin operator."
    )


logger.info(
    "PERFORMANCE OBSERVATION 3 - BROADCAST: %s",
    broadcast_observation
)


# ============================================================
# 27. COLUMN PRUNING OBSERVATION
# ============================================================

logger.info(
    "COLUMN PRUNING OBSERVATION: The hotspot aggregation "
    f"requires only grid_id and total_activity. "
    f"The demonstration reduced the input from "
    f"{full_columns} columns to {pruned_columns} columns "
    "before aggregation, reducing the amount of data that "
    "needs to be carried through the aggregation pipeline."
)


# ============================================================
# 28. OVER-PARTITIONING OBSERVATION
# ============================================================

logger.info(
    "OVER-PARTITIONING OBSERVATION: Increasing partitions "
    f"from {current_partitions} to "
    f"{actual_over_partitions} creates additional task "
    "and scheduling overhead. On a small local dataset, "
    "too many tiny partitions can cost more than the "
    "parallelism they provide."
)


# ============================================================
# 29. EXPLAIN OVER-PARTITIONING CONCEPT
# ============================================================

logger.info(
    "Performance principle: partition count should be "
    "large enough to provide useful parallelism but not "
    "so large that task scheduling and partition management "
    "dominate execution."
)


# ============================================================
# 30. FINAL PERFORMANCE SUMMARY
# ============================================================

print(
    "\n========== SP5 PERFORMANCE SUMMARY =========="
)

print(
    f"Input rows                 : {input_count}"
)

print(
    f"Initial partitions         : {input_partitions}"
)

print(
    f"Partitions after date repartition: "
    f"{after_repartition}"
)

print(
    f"Uncached avg action time   : "
    f"{uncached_average:.4f}s"
)

print(
    f"Cached avg action time     : "
    f"{cached_average:.4f}s"
)

print(
    f"Standard join time         : "
    f"{standard_join_time:.4f}s"
)

print(
    f"Broadcast join time        : "
    f"{broadcast_join_time:.4f}s"
)

print(
    f"Over-partitioned count     : "
    f"{actual_over_partitions}"
)

print(
    f"Over-partitioned time      : "
    f"{over_partition_time:.4f}s"
)


# ============================================================
# 31. LOG FINAL SUMMARY
# ============================================================

logger.info("=" * 60)
logger.info("SP5 FINAL SUMMARY")
logger.info("=" * 60)

logger.info(
    "Hotspot aggregation physical plan: INSPECTED"
)

logger.info(
    "Cache experiment: COMPLETED"
)

logger.info(
    "Repartition experiment: COMPLETED"
)

logger.info(
    "Column pruning experiment: COMPLETED"
)

logger.info(
    "Standard join plan: INSPECTED"
)

logger.info(
    "Broadcast join plan: INSPECTED"
)

logger.info(
    "Over-partitioning experiment: COMPLETED"
)

logger.info(
    "Performance observation 1: CACHE - RECORDED"
)

logger.info(
    "Performance observation 2: REPARTITIONING - RECORDED"
)

logger.info(
    "Performance observation 3: BROADCAST - RECORDED"
)

logger.info(
    "Additional observation: COLUMN PRUNING - RECORDED"
)

logger.info(
    "Additional observation: OVER-PARTITIONING - RECORDED"
)

logger.info(
    "SP5 completed successfully"
)

logger.info("=" * 60)


# ============================================================
# 32. CLEANUP
#
# We can unpersist cached DataFrames after all experiments.
# ============================================================

cached_df.unpersist()

logger.info(
    "Cached DataFrame unpersisted after SP5 experiments"
)


print(
    f"\nSP5 log saved to: {log_file}"
)

print("=" * 60)