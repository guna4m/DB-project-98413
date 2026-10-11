"""
Databricks / PySpark optimization cheat sheet

This file explains the most common optimization techniques in a simple way.
It also shows:
- what each technique does
- how it is different from the others
- when to use it
- how to enable it by default
- how to use it at runtime
- which combinations are redundant

This is a learning reference for Databricks Delta tables and PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Databricks_Optimization_Reference").getOrCreate()

# -----------------------------------------------------------------------------
# 1) Broadcast join
# -----------------------------------------------------------------------------
# What it is:
#   Spark sends the small side of a join to the executors that run the join.
#   Each executor has a local broadcast copy; tasks on that executor can reuse it.
#   Spark does not make a separate full copy for every task.
#
# How it can improve performance:
#   Without broadcast, Spark may shuffle both sides so matching keys meet.
#   With broadcast, each task joins its local large-data partition with the local
#   small-table copy, often avoiding the join shuffle.
#   Even on one worker node, this may avoid repartitioning and shuffle I/O, though
#   the benefit is often greater across multiple worker nodes.
#
# When to use:
#   One side is small enough to fit comfortably in memory on each executor.
#   Example: a large orders table joined to a small customer dimension.
#   Select only the needed columns (and filter first) to keep the broadcast small.
#
# Difference from others:
#   Not a write optimization. It is a join optimization.
#   It helps before or during the join, not after writing.
#   It is not the same as df.cache()/persist(): a broadcast is built for a join
#   execution and is not a promise to cache the DataFrame for future queries.
#
# Lifetime and reuse:
#   The broadcast is distributed to participating executors for the join. Tasks
#   on each executor reuse its local copy. Spark manages cleanup; do not assume
#   the copy stays available for a separate later query. A later join execution
#   may need to broadcast the data again.
#   Broadcast copies are per executor, not one shared memory copy per worker node.
#
# Default enable:
#   There is no per-table broadcast setting. Spark may choose broadcast
#   automatically using size statistics and spark.sql.autoBroadcastJoinThreshold.
#   Use F.broadcast(...) as a hint for a specific join when appropriate.
#   Check the plan with joined_df.explain("formatted"); an explicit hint does not
#   make an oversized table safe to broadcast.
#
# Runtime example:
customer_dim = spark.createDataFrame(
    [
        (1, "Alice", "US"),
        (2, "Bob", "US"),
        (3, "Charlie", "EU"),
    ],
    ["customer_id", "customer_name", "region"],
)

orders_fact = spark.createDataFrame(
    [
        (101, 1, 200.0),
        (102, 2, 150.0),
        (103, 3, 500.0),
        (104, 1, 420.0),
        (105, 2, 120.0),
    ],
    ["order_id", "customer_id", "amount"],
)

# Broadcast only the columns needed by this join. For this tiny example, the
# complete customer dimension is small; production data needs size judgment.
small_customer_dim = customer_dim.select("customer_id", "customer_name", "region")
broadcast_join_df = orders_fact.join(F.broadcast(small_customer_dim), "customer_id", "left")
print("\n=== Broadcast join example ===")
broadcast_join_df.show()

# A left_semi join keeps rows from orders_fact whose customer_id exists in the broadcast DataFrame.
# It does not add columns from customer_ids, so this is ideal for filtering by membership.
customer_ids = customer_dim.select("customer_id").distinct()

orders_for_known_customers = orders_fact.join(
    F.broadcast(customer_ids),
    on="customer_id",
    how="left_semi"
)

# -----------------------------------------------------------------------------
# 2) Repartition
# -----------------------------------------------------------------------------
# What it is:
#   Changes the number of partitions in a DataFrame.
#   Usually triggers a shuffle.
#
# When to use:
#   Data is large and you want more parallelism.
#   You want to match a target partition count before a write or join.
#
# Difference from coalesce:
#   repartition() usually requires shuffle and can increase or decrease partition count.
#   coalesce() is mostly for reducing partitions without a full shuffle.
#
# Runtime example:
repartitioned_df = orders_fact.repartition(4)
print("\n=== Repartition example ===")
print("repartitioned partitions:", repartitioned_df.rdd.getNumPartitions())

# -----------------------------------------------------------------------------
# 3) Coalesce
# -----------------------------------------------------------------------------
# What it is:
#   Reduces number of partitions with minimal shuffle.
#
# When to use:
#   You already have a reasonable number of partitions and just want fewer partitions.
#   Example: reduce too many small output files.
#
# Difference from repartition:
#   coalesce() is not a full shuffle.
#   It is usually only for reducing partition count.
#
# Runtime example:
coalesced_df = repartitioned_df.coalesce(2)
print("\n=== Coalesce example ===")
print("coalesced partitions:", coalesced_df.rdd.getNumPartitions())

# -----------------------------------------------------------------------------
# 4) optimizeWrite
# -----------------------------------------------------------------------------
# What it is:
#   Delta write-time optimization.
#   It helps reduce the small file problem during write.
#
# When to use:
#   When writing many partitions or frequent incremental writes.
#
# Difference from OPTIMIZE:
#   optimizeWrite is during the write itself.
#   OPTIMIZE is a maintenance step after the data is already written.
#
# Runtime example:
# This is used when writing a DataFrame to a Delta table.
# Example only: final_df.write.format("delta").mode("overwrite").option("optimizeWrite", True).saveAsTable("sales_opt_write")
#
# Default enable:
#   On a table, use:
#   ALTER TABLE sales_opt_write SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
#
# Redundant combinations:
#   If a table already has delta.autoOptimize.optimizeWrite = true,
#   manually setting .option("optimizeWrite", True) on every write is usually redundant.
#
# Runtime example in script form:
# sales_df.write.format("delta").mode("overwrite").option("optimizeWrite", True).saveAsTable("sales_opt_write")

# -----------------------------------------------------------------------------
# 5) Auto optimize / auto compact
# -----------------------------------------------------------------------------
# What it is:
#   These are table-level defaults that automatically reduce small files.
#
# When to use:
#   When the table is frequently written and you want Databricks to handle compaction by default.
#
# Default enable (SQL):
spark.sql("ALTER TABLE sales_fact SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')")
spark.sql("ALTER TABLE sales_fact SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')")

# Why it matters:
# - automatically reduces file count for recurring writes
# - helps avoid too many tiny files
# - keeps the table healthier without manually running OPTIMIZE every time
#
# Redundant combination:
#   If autoOptimize is enabled at table level, then .option("optimizeWrite", True)
#   is often not needed for that table.

# -----------------------------------------------------------------------------
# 6) Delta OPTIMIZE
# -----------------------------------------------------------------------------
# What it is:
#   A maintenance command that compacts small files after they have been written.
#
# When to use:
#   After many writes, or when a table has too many small files.
#
# Difference from optimizeWrite:
#   optimizeWrite = during write
#   OPTIMIZE = after write
#
# Runtime / SQL example:
spark.sql("OPTIMIZE sales_fact")

# -----------------------------------------------------------------------------
# 7) Z-Ordering
# -----------------------------------------------------------------------------
# What it is:
#   Groups related data together based on chosen columns.
#
# When to use:
#   Very common filters: customer_id, region, date, product_id
#
# Difference from CLUSTER BY:
#   CLUSTER BY arranges the table layout at creation time.
#   Z-ORDER is a Delta optimization that helps query pruning on repeated filter columns.
#
# Runtime / SQL example:
spark.sql("OPTIMIZE sales_fact ZORDER BY (customer_id, order_date)")

# -----------------------------------------------------------------------------
# 8) CLUSTER BY
# -----------------------------------------------------------------------------
# What it is:
#   Physically sorts or groups rows by a key when creating the table.
#
# When to use:
#   For tables that are frequently filtered on a key like customer_id, region, date.
#
# Difference from Z-ORDER:
#   CLUSTER BY is a table layout choice while creating a table.
#   Z-ORDER is a Delta optimization applied on existing tables.
#
# Example SQL:
spark.sql("""
CREATE TABLE IF NOT EXISTS sales_clustered
CLUSTER BY (customer_id)
AS
SELECT *
FROM sales_fact
""")

# -----------------------------------------------------------------------------
# 9) Early filtering
# -----------------------------------------------------------------------------
# What it is:
#   Apply filters before expensive joins or writes.
#
# Why it matters:
#   Smaller DataFrame = less shuffle, less memory, less time.
#
# When to use:
#   Always. Filter data early if possible.
#
# Example:
filtered_orders = orders_fact.filter(F.col("customer_id") == 1)
print("\n=== Early filtered dataset ===")
filtered_orders.show()

# -----------------------------------------------------------------------------
# 10) Partition count and file size tuning
# -----------------------------------------------------------------------------
# What it is:
#   You want enough partitions for parallelism, but not too many very small files.
#
# When to use:
#   Big datasets and large jobs.
#
# Beginner rule:
#   - too few partitions => slow, skewed tasks
#   - too many partitions => many tiny files, overhead
#   - choose a reasonable number based on cluster size and row volume
#
# Example:
large_df = orders_fact.repartition(8)
print("\n=== Partition tuning example ===")
print("partition count:", large_df.rdd.getNumPartitions())

# -----------------------------------------------------------------------------
# 11) Simple summary table: what to use and when
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Technique          Use when                                  Why it helps
# -----------------------------------------------------------------------------
# Broadcast join     Small dimension + big fact join            Avoid shuffle
# Repartition        Need more or fewer partitions             Control parallelism / output shape
# Coalesce           Need fewer partitions without shuffle     Reduce partition count cheaply
# optimizeWrite      Write-time Delta tuning                  Reduce small files on write
# autoOptimize       Table default tuning                     Automatic compaction for future writes
# OPTIMIZE           Post-write compaction                     Fix small-file issue after load
# Z-ORDER            Frequent filter columns                  Reduce scan volume
# CLUSTER BY         Tables filtered on a key                Better layout and pruning
# Early filter       Before join or write                     Less work and less shuffle
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# 12) Redundant or non-essential combinations
# -----------------------------------------------------------------------------
# 1) autoOptimize + optimizeWrite at runtime
#    Usually redundant for the same table.
#    Use either table-level default or runtime option, not both unless you intentionally want to override behavior.
#
# 2) repartition() followed by coalesce()
#    This is often unnecessary if the final target is already known.
#    Use only the one that matches your goal.
#
# 3) CLUSTER BY and Z-ORDER are not the same
#    Both can be useful, but they solve different problems.
#    One is table layout at creation; the other is Delta compaction/ordering for reads.
#
# 4) Broadcasting every join is not always wise
#    Very large tables should not be broadcasted.
#    Use it only when the other side is small enough to fit in memory.
#
# 5) High partition count is not always better
#    More partitions can hurt when the dataset is small or when too many tiny files are created.

# -----------------------------------------------------------------------------
# 13) Why these matter in Databricks
# -----------------------------------------------------------------------------
# Databricks Delta tables are often optimized by combining:
# - filtering early
# - proper partitioning
# - broadcast joins for small dimension tables
# - autoOptimize/autoCompact for recurring writes
# - OPTIMIZE and Z-ORDER as maintenance
# - CLUSTER BY for table layout
#
# The key idea is:
#   - reduce data before expensive operations
#   - write in a compact way
#   - maintain the table after repeated writes
#
# Beginner mindset:
#   - broadcast = join optimization
#   - repartition/coalesce = partition tuning
#   - optimizeWrite/autoOptimize = Delta write optimization
#   - OPTIMIZE/Z-ORDER = Delta maintenance optimization
#   - CLUSTER BY = physical table layout optimization

print("\n=== Optimization summary complete ===")

spark.stop()
