"""
Databricks Unity Catalog - Complete PySpark Example with explanations and optimization notes

This script demonstrates:
1. Managed Delta table creation in Unity Catalog
2. External Delta table creation with an explicit S3 path
3. Creating 3 source tables
4. Null handling and dropping nulls
5. Filtering rows
6. Joining tables before writing
7. Deduplicating rows using a window function by latest date
8. Writing final results to both managed and external Delta tables
9. Performance optimization techniques commonly used in Databricks

Why this matters:
- dropna(subset=[...]) removes a row if ANY selected column is NULL
- F.col(...) is the standard way to reference DataFrame columns in PySpark
- Broadcast joins are useful when one side is small
- Repartitioning helps control shuffle and file sizes
- Delta OPTIMIZE and auto-compaction reduce small-file overhead and improve read performance
- CLUSTER BY helps with data layout and pruning for large tables

IMPORTANT:
- This is designed for Databricks with Unity Catalog enabled.
- Replace the bucket path and catalog/schema names to match your environment.
- This script is intended to be run in a Databricks cluster or notebook.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Spark session setup
# -----------------------------------------------------------------------------
# A SparkSession is the entry point for DataFrame APIs in Spark.
# In Databricks, this is already provided by the notebook runtime, but this is
# how you create it in a standalone script.
spark = SparkSession.builder.appName("Databricks_UC_Join_Managed_External").getOrCreate()

CATALOG_NAME = "workspace"
SCHEMA_NAME = "db_work_managed"
EXTERNAL_BASE_PATH = "s3://cts-amzn-s3-db-bucket/db_work"  # us-west2

# -----------------------------------------------------------------------------
# 1) Create catalog and schema (Databricks Unity Catalog)
# -----------------------------------------------------------------------------
# CREATE CATALOG / SCHEMA are used to organize tables in Unity Catalog.
# Managed tables created here will live under the default storage for that catalog/schema.
# If you add a LOCATION path, the table becomes EXTERNAL.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# -----------------------------------------------------------------------------
# 2) Create 3 source tables with nulls and duplicate event rows
# -----------------------------------------------------------------------------
# We create DataFrames directly and then write them to tables.
# These tables intentionally contain nulls and duplicates to show cleanup logic.
customers_raw = spark.createDataFrame(
    [
        (1, "Alice", "alice@abc.com", "US", "2023-01-10"),
        (2, "Bob", None, "US", "2023-01-12"),
        (3, None, "charlie@abc.com", "EU", "2023-01-13"),
        (4, "Diana", "diana@abc.com", "APAC", "2023-01-15"),
        (None, "Unknown", "unknown@abc.com", "US", "2023-01-16"),
        (5, "Eve", "eve@abc.com", "EU", None),
    ],
    ["customer_id", "customer_name", "email", "region", "signup_date"],
)

orders_raw = spark.createDataFrame(
    [
        (101, 1, 250.0, "2024-01-05", "PAID"),
        (102, 2, None, "2024-01-08", "PENDING"),
        (103, 2, 400.0, "2024-01-09", "PAID"),
        (104, None, 90.0, "2024-01-11", "PAID"),
        (105, 4, 150.0, "2024-01-12", "CANCELLED"),
        (106, 5, 500.0, "2024-01-15", "PAID"),
        (107, 5, 320.0, "2024-01-13", "PAID"),
    ],
    ["order_id", "customer_id", "amount", "order_date", "order_status"],
)

customer_events_raw = spark.createDataFrame(
    [
        (1, "LOGIN", "2024-01-01", "2024-01-01 09:00:00"),
        (1, "PURCHASE", "2024-01-02", "2024-01-02 10:00:00"),
        (1, "PURCHASE", "2024-01-03", "2024-01-03 11:00:00"),
        (2, "LOGIN", "2024-01-02", "2024-01-02 08:00:00"),
        (2, "LOGIN", "2024-01-04", "2024-01-04 15:30:00"),
        (4, "PURCHASE", "2024-01-03", "2024-01-03 09:00:00"),
        (4, "PURCHASE", "2024-01-03", "2024-01-03 18:00:00"),
        (5, None, "2024-01-04", "2024-01-04 12:00:00"),
    ],
    ["customer_id", "event_type", "event_date", "event_created_at"],
)

# -----------------------------------------------------------------------------
# 3) Null handling and data cleaning
# -----------------------------------------------------------------------------
# dropna(subset=[...]) means: remove a row if ANY selected field is NULL.
# Default behavior of dropna is how='any'; if any of the selected columns is null,
# that row is dropped.
#
# Example:
# customers_raw.dropna(subset=["customer_id", "customer_name", "signup_date"])
# -> drops the row if customer_id OR customer_name OR signup_date is NULL.
#
# Alternative: use filter with F.col(...).isNotNull() for more explicit logic.
customers_clean = customers_raw.dropna(subset=["customer_id", "customer_name", "signup_date"])

# orders_clean: we want valid customer_id, valid order_date, valid amount,
# and only statuses we care about.
orders_clean = orders_raw.filter(
    F.col("customer_id").isNotNull() &
    F.col("order_date").isNotNull() &
    F.col("order_status").isin("PAID", "PENDING")
).dropna(subset=["customer_id", "order_date", "amount"])

customer_events_clean = customer_events_raw.filter(
    F.col("customer_id").isNotNull() &
    F.col("event_date").isNotNull() &
    F.col("event_type").isNotNull()
)

# -----------------------------------------------------------------------------
# Why use F.col?
# -----------------------------------------------------------------------------
# F.col("customer_id") is the standard PySpark way to reference a DataFrame column.
# It creates a Column object so Spark can build a logical expression plan.
#
# Examples:
# F.col("customer_id").isNotNull()
# F.col("amount") + 10
# F.col("order_status").isin(["PAID", "PENDING"])
#
# Alternatives:
# 1) df["customer_id"]
# 2) "customer_id IS NOT NULL" (string SQL expression)
# 3) df.filter("customer_id IS NOT NULL")
#
# Why F.col is preferred:
# - it is clear and readable
# - it works with many DataFrame functions
# - it integrates well with Spark SQL expression building

# -----------------------------------------------------------------------------
# 4) Deduplicate by choosing the latest event per customer
# -----------------------------------------------------------------------------
# Window functions are used when you want to rank or choose one row per group.
# Here we want the latest event by customer based on event_date and time.
window_spec = (
    Window.partitionBy("customer_id")
    .orderBy(F.col("event_date").desc(), F.col("event_created_at").desc())
)

latest_customer_event = (
    customer_events_clean
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# -----------------------------------------------------------------------------
# 5) Join the three tables before writing
# -----------------------------------------------------------------------------
# A join combines data from multiple tables based on a common key.
# We join customers with orders and then with event data.
#
# Why left join?
# - We want to keep all customer rows even if some do not have an order or event record.
#
# Why filter after join?
# - cleanup is often easier after join, especially when nulls appear from unmatched rows.
final_df = (
    customers_clean
    .join(orders_clean, on="customer_id", how="left")
    .join(latest_customer_event, on="customer_id", how="left")
    .filter(F.col("customer_name").isNotNull())
    .filter(F.col("region").isin("US", "EU", "APAC"))
    .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
    .select(
        "customer_id",
        "customer_name",
        "email",
        "region",
        "signup_date",
        "order_id",
        "order_date",
        "order_status",
        "amount",
        "event_type",
        "event_date",
        "event_created_at",
    )
    .orderBy(F.col("customer_id").asc(), F.col("order_date").asc())
)

print("\n=== Final joined data ===")
final_df.show(truncate=False)

# -----------------------------------------------------------------------------
# 5A) Remove duplicates using window function (PySpark)
# -----------------------------------------------------------------------------
# This is the safest pattern when you want to keep the required record based on a rule,
# such as: "keep the newest event by customer_id" or "keep the highest amount".
# dropDuplicates() is not enough here because it removes duplicates without honoring a business rule.
customer_events_window = (
    customer_events_raw
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("customer_id", "event_type")
            .orderBy(F.col("event_date").desc(), F.col("event_created_at").desc())
        )
    )
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print("\n=== Deduplicated customer events (keep latest per customer + event type) ===")
customer_events_window.show(truncate=False)

# Alternative: if there are exact duplicate rows and you do NOT care which one is kept,
# use dropDuplicates() with a subset of columns.
exact_duplicates_removed = customer_events_raw.dropDuplicates(["customer_id", "event_type", "event_date", "event_created_at"])
print("\n=== Exact duplicate removal using dropDuplicates ===")
exact_duplicates_removed.show(truncate=False)

# -----------------------------------------------------------------------------
# 5B) Remove duplicates using Spark SQL window function
# -----------------------------------------------------------------------------
# This is useful when you want to keep the newest row in SQL without doing a full delete in place.
# The usual pattern is: rank records, keep only rn = 1, then write to a cleaned table.
#
# Example:
# WITH ranked_events AS (
#     SELECT *,
#            ROW_NUMBER() OVER (
#                PARTITION BY customer_id, event_type
#                ORDER BY event_date DESC, event_created_at DESC
#            ) AS rn
#     FROM customer_events_raw
# )
# SELECT *
# FROM ranked_events
# WHERE rn = 1;
#
# This keeps the latest event per business key and is far more intentional than dropDuplicates().

spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_events_ranked AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY customer_id, event_type
           ORDER BY event_date DESC, event_created_at DESC
       ) AS rn
FROM customer_events_raw
""")

latest_events_sql = spark.sql("""
SELECT *
FROM customer_events_ranked
WHERE rn = 1
""")
print("\n=== Latest events kept using SQL window function ===")
latest_events_sql.show(truncate=False)

# -----------------------------------------------------------------------------
# 5C) Delete duplicates from an existing Delta table using a targeted rule
# -----------------------------------------------------------------------------
# In Databricks, a common approach is NOT to directly delete every duplicate in place.
# Instead, you either:
# 1) create a clean table from the deduplicated result, or
# 2) use DELETE/MERGE to remove rows you no longer want while keeping the latest valid row.
#
# Example pattern for targeted cleanup (conceptual):
# WITH ranked_events AS (
#     SELECT *,
#            ROW_NUMBER() OVER (
#                PARTITION BY customer_id, event_type
#                ORDER BY event_date DESC, event_created_at DESC
#            ) AS rn
#     FROM customer_events_raw
# )
# DELETE FROM customer_events_raw
# WHERE customer_id IN (
#     SELECT customer_id
#     FROM ranked_events
#     WHERE rn > 1
# );
#
# This is more controlled than dropDuplicates(), because it preserves the row you want
# (latest date) and removes the older versions only.

# -----------------------------------------------------------------------------
# 6) Write managed table to UC managed storage
# -----------------------------------------------------------------------------
# A managed table has no LOCATION clause.
# Databricks stores the data in the default storage for the current catalog/schema.
managed_table_name = "customer_order_summary_managed"
final_df.write.format("delta").mode("overwrite").saveAsTable(managed_table_name)
print(f"Managed table created: {managed_table_name}")

# -----------------------------------------------------------------------------
# 7) Write external table to a custom S3 path
# -----------------------------------------------------------------------------
# An external table is created by passing a path or using LOCATION in SQL.
# This is useful when you want to control the storage path explicitly.
external_table_name = "customer_order_summary_external"
external_path = f"{EXTERNAL_BASE_PATH}/customer_order_summary_external"
final_df.write.format("delta").mode("overwrite").option("path", external_path).saveAsTable(external_table_name)
print(f"External table created: {external_table_name} at {external_path}")

# -----------------------------------------------------------------------------
# 8) Inspect table metadata
# -----------------------------------------------------------------------------
# DESCRIBE TABLE EXTENDED shows where the table lives and whether it is managed or external.
print("\n=== Managed table metadata ===")
spark.sql(f"DESCRIBE TABLE EXTENDED {managed_table_name}").show(truncate=False)

print("\n=== External table metadata ===")
spark.sql(f"DESCRIBE TABLE EXTENDED {external_table_name}").show(truncate=False)

# -----------------------------------------------------------------------------
# 9) Performance optimization techniques - executable examples
# -----------------------------------------------------------------------------
# These are real, runnable examples you can use in Databricks. The earlier section was
# kept as comments for explanation; this block turns the ideas into working PySpark/SQL code.

print("\n=== Performance optimization examples ===")

# 9.1 Broadcast join example
customer_dim_small = customers_clean.select("customer_id", "customer_name", "region")
orders_fact_large = orders_clean
broadcast_join_df = orders_fact_large.join(F.broadcast(customer_dim_small), "customer_id", "left")
print("\n=== Broadcast join sample ===")
broadcast_join_df.show(10, truncate=False)

# 9.2 Repartition and coalesce example
repartitioned_df = final_df.repartition(4)
coalesced_df = final_df.coalesce(2)
print("\n=== Repartitioned sample ===")
repartitioned_df.show(5, truncate=False)
print("\n=== Coalesced sample ===")
coalesced_df.show(5, truncate=False)

# 9.3 Write with optimizeWrite enabled
optimized_write_df = final_df.repartition(4)
optimized_write_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("optimizeWrite", True) \
    .saveAsTable("customer_order_summary_optimized_write")
print("Created table: customer_order_summary_optimized_write with optimizeWrite enabled")

# 9.4 Auto optimize and auto compact on an existing table
spark.sql("ALTER TABLE customer_order_summary_managed SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')")
spark.sql("ALTER TABLE customer_order_summary_managed SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')")
print("Applied autoOptimize and autoCompact table properties to customer_order_summary_managed")

# 9.5 Delta OPTIMIZE + Z-ORDER example
spark.sql("OPTIMIZE customer_order_summary_managed ZORDER BY (customer_id, order_date)")
print("Ran OPTIMIZE ZORDER for customer_order_summary_managed")

# 9.6 CLUSTER BY example in SQL (create a new clustered table)
# This is a table-layout optimization; it is best used when creating the table from a query.
spark.sql("""
CREATE TABLE IF NOT EXISTS customer_order_summary_clustered
CLUSTER BY (customer_id)
AS
SELECT *
FROM customer_order_summary_managed
""")
print("Created clustered table: customer_order_summary_clustered")

# 9.7 Data filtering before writing to reduce work and avoid unnecessary shuffle
filtered_for_write = final_df.filter(F.col("amount") > 0)
filtered_for_write.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customer_order_summary_filtered")
print("Created filtered table: customer_order_summary_filtered")

# 9.8 Delta table maintenance examples
# Use these in an actual production workload when the table is large and frequently written.
# spark.sql("OPTIMIZE customer_order_summary_filtered ZORDER BY (customer_id, order_date)")
# spark.sql("VACUUM customer_order_summary_filtered RETAIN 168 HOURS")

# -----------------------------------------------------------------------------
# 10) Summary of when to use each technique
# -----------------------------------------------------------------------------
# - broadcast() for small dimension tables in large joins
# - repartition() when you want more parallelism or a larger target number of files
# - coalesce() when you want to reduce partitions without a full shuffle
# - optimizeWrite for write-time Delta tuning
# - autoOptimize / autoCompact for table-level tuning on frequent writes
# - OPTIMIZE ZORDER BY for post-write maintenance on large tables
# - CLUSTER BY for table layout and pruning on commonly filtered columns
# - filter early to reduce data before expensive transformations
#
# Important note:
# - repartition and coalesce are not the same thing
# - optimizeWrite and OPTIMIZE are not the same thing
# - if you already enabled autoOptimize/autoCompact at the table level, manually setting
#   .option("optimizeWrite", True) on every write may be redundant for that table

# -----------------------------------------------------------------------------
# 11) End of script
# -----------------------------------------------------------------------------
spark.stop()
