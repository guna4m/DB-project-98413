-- Databricks Unity Catalog - Complete SQL Example
-- This script demonstrates:
-- 1. Managed Delta table creation in UC
-- 2. External Delta table creation with S3 LOCATION
-- 3. Three source tables
-- 4. Null handling and filtering
-- 5. Joining tables before writing
-- 6. Deduplication using ROW_NUMBER() over a date-based window
-- 7. Writing results to both managed and external Delta tables
--
-- IMPORTANT:
-- - Replace catalog/schema names and S3 paths with your actual values.
-- - Run in Databricks SQL or Databricks notebook SQL cells.
-- - This is for Databricks with Unity Catalog enabled.

CREATE CATALOG IF NOT EXISTS demo_uc;
USE CATALOG demo_uc;

CREATE SCHEMA IF NOT EXISTS delta_demo;
USE SCHEMA delta_demo;

-- -----------------------------------------------------------------------------
-- 1) Create 3 raw tables
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE customers_raw (
    customer_id INT,
    customer_name STRING,
    email STRING,
    region STRING,
    signup_date DATE
) USING DELTA;

INSERT INTO customers_raw VALUES
    (1, 'Alice', 'alice@abc.com', 'US', DATE '2023-01-10'),
    (2, 'Bob', NULL, 'US', DATE '2023-01-12'),
    (3, NULL, 'charlie@abc.com', 'EU', DATE '2023-01-13'),
    (4, 'Diana', 'diana@abc.com', 'APAC', DATE '2023-01-15'),
    (NULL, 'Unknown', 'unknown@abc.com', 'US', DATE '2023-01-16'),
    (5, 'Eve', 'eve@abc.com', 'EU', NULL);

CREATE OR REPLACE TABLE orders_raw (
    order_id INT,
    customer_id INT,
    amount DOUBLE,
    order_date DATE,
    order_status STRING
) USING DELTA;

INSERT INTO orders_raw VALUES
    (101, 1, 250.0, DATE '2024-01-05', 'PAID'),
    (102, 2, NULL, DATE '2024-01-08', 'PENDING'),
    (103, 2, 400.0, DATE '2024-01-09', 'PAID'),
    (104, NULL, 90.0, DATE '2024-01-11', 'PAID'),
    (105, 4, 150.0, DATE '2024-01-12', 'CANCELLED'),
    (106, 5, 500.0, DATE '2024-01-15', 'PAID'),
    (107, 5, 320.0, DATE '2024-01-13', 'PAID');

CREATE OR REPLACE TABLE customer_events_raw (
    customer_id INT,
    event_type STRING,
    event_date DATE,
    event_created_at STRING
) USING DELTA;

INSERT INTO customer_events_raw VALUES
    (1, 'LOGIN', DATE '2024-01-01', '2024-01-01 09:00:00'),
    (1, 'PURCHASE', DATE '2024-01-02', '2024-01-02 10:00:00'),
    (1, 'PURCHASE', DATE '2024-01-03', '2024-01-03 11:00:00'),
    (2, 'LOGIN', DATE '2024-01-02', '2024-01-02 08:00:00'),
    (2, 'LOGIN', DATE '2024-01-04', '2024-01-04 15:30:00'),
    (4, 'PURCHASE', DATE '2024-01-03', '2024-01-03 09:00:00'),
    (4, 'PURCHASE', DATE '2024-01-03', '2024-01-03 18:00:00'),
    (5, NULL, DATE '2024-01-04', '2024-01-04 12:00:00');

-- -----------------------------------------------------------------------------
-- 2) Clean source tables
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE customers_clean AS
SELECT
    customer_id,
    customer_name,
    email,
    region,
    signup_date
FROM customers_raw
WHERE customer_id IS NOT NULL
  AND customer_name IS NOT NULL
  AND signup_date IS NOT NULL;

CREATE OR REPLACE TABLE orders_clean AS
SELECT
    order_id,
    customer_id,
    amount,
    order_date,
    order_status
FROM orders_raw
WHERE customer_id IS NOT NULL
  AND order_date IS NOT NULL
  AND order_status IN ('PAID', 'PENDING')
  AND amount IS NOT NULL;

CREATE OR REPLACE TABLE customer_events_clean AS
SELECT
    customer_id,
    event_type,
    event_date,
    event_created_at
FROM customer_events_raw
WHERE customer_id IS NOT NULL
  AND event_date IS NOT NULL
  AND event_type IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 3) Deduplicate latest event by customer using a window function
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE latest_customer_event AS
SELECT *
FROM (
    SELECT
        customer_id,
        event_type,
        event_date,
        event_created_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY event_date DESC, event_created_at DESC
        ) AS row_num
    FROM customer_events_clean
)
WHERE row_num = 1;

-- -----------------------------------------------------------------------------
-- 4) Join the cleaned tables before writing the final result
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE customer_order_summary_managed AS
SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.region,
    c.signup_date,
    o.order_id,
    o.order_date,
    o.order_status,
    COALESCE(o.amount, 0.0) AS amount,
    l.event_type,
    l.event_date AS latest_event_date,
    l.event_created_at AS latest_event_created_at
FROM customers_clean c
LEFT JOIN orders_clean o
    ON c.customer_id = o.customer_id
LEFT JOIN latest_customer_event l
    ON c.customer_id = l.customer_id
WHERE c.region IN ('US', 'EU', 'APAC')
ORDER BY c.customer_id, o.order_date;

-- -----------------------------------------------------------------------------
-- 5) Create external output table with explicit S3 LOCATION
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE customer_order_summary_external
USING DELTA
LOCATION 's3://my-bucket/data/databricks_uc_demo/customer_order_summary_external'
AS
SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.region,
    c.signup_date,
    o.order_id,
    o.order_date,
    o.order_status,
    COALESCE(o.amount, 0.0) AS amount,
    l.event_type,
    l.event_date AS latest_event_date,
    l.event_created_at AS latest_event_created_at
FROM customers_clean c
LEFT JOIN orders_clean o
    ON c.customer_id = o.customer_id
LEFT JOIN latest_customer_event l
    ON c.customer_id = l.customer_id
WHERE c.region IN ('US', 'EU', 'APAC')
ORDER BY c.customer_id, o.order_date;

-- -----------------------------------------------------------------------------
-- 6) View results
-- -----------------------------------------------------------------------------
SELECT *
FROM customer_order_summary_managed;

SELECT *
FROM customer_order_summary_external;

-- -----------------------------------------------------------------------------
-- 7) Inspect metadata
-- -----------------------------------------------------------------------------
DESCRIBE TABLE EXTENDED customer_order_summary_managed;
DESCRIBE TABLE EXTENDED customer_order_summary_external;

-- End of script
