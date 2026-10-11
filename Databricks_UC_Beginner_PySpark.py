"""
Databricks beginner-friendly PySpark example

This script is intentionally simple and easy to follow.
It shows:
1. how to create a catalog and schema in Unity Catalog
2. how to create raw data with nulls and duplicates
3. how to clean data
4. how to join tables
5. how to keep only the latest event per customer using a window function
6. how to write both managed and external Delta tables
7. how to use a few optimization ideas in practice

Before running:
- Replace catalog/schema names if needed
- Replace S3 external path with your real bucket path
- Run this in Databricks with Unity Catalog enabled
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Databricks_Beginner_Example").getOrCreate()

CATALOG_NAME = "workspace"
SCHEMA_NAME = "db_work_managed"
EXTERNAL_PATH = "s3://cts-amzn-s3-db-bucket/db_work/customer_order_summary_external"

# 1) Create catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# 2) Create raw tables with sample data
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

# 3) Clean the customer data
# dropna(subset=[...]) removes a row if ANY selected column is NULL.
customers_clean = customers_raw.dropna(subset=["customer_id", "customer_name", "signup_date"])

# 4) Clean orders data: remove nulls and invalid statuses
orders_clean = orders_raw.filter(
    F.col("customer_id").isNotNull() &
    F.col("order_date").isNotNull() &
    F.col("order_status").isin(["PAID", "PENDING"])
).dropna(subset=["customer_id", "order_date", "amount"])

# 5) Clean events data: remove rows with missing key values
customer_events_clean = customer_events_raw.filter(
    F.col("customer_id").isNotNull() &
    F.col("event_date").isNotNull() &
    F.col("event_type").isNotNull()
)

# 6) Keep only the latest event per customer
window_spec = (
    Window.partitionBy("customer_id")
    .orderBy(F.col("event_date").desc(), F.col("event_created_at").desc())
)

latest_event = (
    customer_events_clean
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# 7) Join the tables together
final_df = (
    customers_clean
    .join(orders_clean, on="customer_id", how="left")
    .join(latest_event, on="customer_id", how="left")
    .filter(F.col("customer_name").isNotNull())
    .filter(F.col("region").isin(["US", "EU", "APAC"]))
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
    .orderBy(F.col("customer_id"), F.col("order_date"))
)

final_df.show(truncate=False)

# 8) Write managed table
managed_table = "customer_order_summary_managed"
final_df.write.format("delta").mode("overwrite").saveAsTable(managed_table)
print(f"Managed table created: {managed_table}")

# 9) Write external table with S3 path
external_table = "customer_order_summary_external"
final_df.write.format("delta").mode("overwrite").option("path", EXTERNAL_PATH).saveAsTable(external_table)
print(f"External table created: {external_table} at {EXTERNAL_PATH}")

# 10) Metadata check
print("\nManaged table metadata:")
spark.sql(f"DESCRIBE TABLE EXTENDED {managed_table}").show(truncate=False)

print("\nExternal table metadata:")
spark.sql(f"DESCRIBE TABLE EXTENDED {external_table}").show(truncate=False)

# 11) Optimization examples
# These are small examples to understand common Databricks tuning ideas.
# They are not necessary for a tiny sample, but they are important in real workloads.

# Broadcast join example
# large_df = orders_clean
# small_df = customers_clean
# result = large_df.join(F.broadcast(small_df), "customer_id", "left")

# Repartition to reduce too many tiny partitions
# final_df = final_df.repartition(4)

# Optimize write config
# final_df.write.format("delta").mode("overwrite").option("optimizeWrite", True).saveAsTable(managed_table)

# Auto compact / auto optimize on an existing table
# spark.sql("ALTER TABLE customer_order_summary_managed SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')")
# spark.sql("ALTER TABLE customer_order_summary_managed SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')")

# Optimize and Z-order an existing table
# spark.sql("OPTIMIZE customer_order_summary_managed ZORDER BY (customer_id, order_date)")

spark.stop()
