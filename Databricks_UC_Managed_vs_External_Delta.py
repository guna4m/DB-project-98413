"""
Databricks Unity Catalog - Complete PySpark Example

This script demonstrates:
1. Managed Delta table creation in UC
2. External Delta table creation with an explicit S3 path
3. Creating 3 source tables
4. Null handling and dropping nulls
5. Filtering rows
6. Joining tables before writing
7. Deduplicating rows using a window function by latest date
8. Writing final results to both managed and external Delta tables

IMPORTANT:
- This is designed for Databricks with Unity Catalog enabled.
- Replace the bucket path and catalog/schema names to match your environment.
- This script is intended to be run in a Databricks cluster or notebook.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Databricks_UC_Join_Managed_External").getOrCreate()

CATALOG_NAME = "demo_uc"
SCHEMA_NAME = "delta_demo"
EXTERNAL_BASE_PATH = "s3://my-bucket/data/databricks_uc_demo"  # replace with your real bucket path

# -----------------------------------------------------------------------------
# 1) Create catalog and schema (Databricks Unity Catalog)
# -----------------------------------------------------------------------------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# -----------------------------------------------------------------------------
# 2) Create 3 source tables with nulls and duplicate event rows
# -----------------------------------------------------------------------------
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
customers_clean = customers_raw.dropna(subset=["customer_id", "customer_name", "signup_date"])

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
# 4) Deduplicate by choosing latest event per customer
# -----------------------------------------------------------------------------
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
final_df = (
    customers_clean
    .join(orders_clean, on="customer_id", how="left")
    .join(latest_customer_event, on="customer_id", how="left")
    .filter(F.col("customer_name").isNotNull())
    .filter(F.col("region").isin("US", "EU", "APAC"))
    .withColumn("amount", F.when(F.col("amount").isNull(), F.lit(0.0)).otherwise(F.col("amount")))
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
    .orderBy("customer_id", "order_date")
)

print("\n=== Final joined data ===")
final_df.show(truncate=False)

# -----------------------------------------------------------------------------
# 6) Write managed table to UC managed storage
# -----------------------------------------------------------------------------
managed_table_name = "customer_order_summary_managed"
final_df.write.format("delta").mode("overwrite").saveAsTable(managed_table_name)
print(f"Managed table created: {managed_table_name}")

# -----------------------------------------------------------------------------
# 7) Write external table to a custom S3 path
# -----------------------------------------------------------------------------
external_table_name = "customer_order_summary_external"
external_path = f"{EXTERNAL_BASE_PATH}/customer_order_summary_external"
final_df.write.format("delta").mode("overwrite").option("path", external_path).saveAsTable(external_table_name)
print(f"External table created: {external_table_name} at {external_path}")

# -----------------------------------------------------------------------------
# 8) Inspect table metadata
# -----------------------------------------------------------------------------
print("\n=== Managed table metadata ===")
spark.sql(f"DESCRIBE TABLE EXTENDED {managed_table_name}").show(truncate=False)

print("\n=== External table metadata ===")
spark.sql(f"DESCRIBE TABLE EXTENDED {external_table_name}").show(truncate=False)

# End of script
spark.stop()
