-- Databricks beginner-friendly SQL example
-- This script creates cleaned and joined data and writes both managed and external tables.

CREATE CATALOG IF NOT EXISTS workspace;
USE CATALOG workspace;

CREATE SCHEMA IF NOT EXISTS db_work_managed;
USE SCHEMA db_work_managed;

-- 1) Raw tables with sample data
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

-- 2) Clean customer data
CREATE OR REPLACE TABLE customers_clean AS
SELECT *
FROM customers_raw
WHERE customer_id IS NOT NULL
  AND customer_name IS NOT NULL
  AND signup_date IS NOT NULL;

-- 3) Clean order data
CREATE OR REPLACE TABLE orders_clean AS
SELECT *
FROM orders_raw
WHERE customer_id IS NOT NULL
  AND order_date IS NOT NULL
  AND amount IS NOT NULL
  AND order_status IN ('PAID', 'PENDING');

-- 4) Clean event data
CREATE OR REPLACE TABLE customer_events_clean AS
SELECT *
FROM customer_events_raw
WHERE customer_id IS NOT NULL
  AND event_date IS NOT NULL
  AND event_type IS NOT NULL;

-- 5) Deduplicate by keeping the latest event per customer
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
        ) AS rn
    FROM customer_events_clean
) t
WHERE rn = 1;

-- 6) Final managed table
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
WHERE c.region IN ('US', 'EU', 'APAC');

-- 7) Final external table with S3 LOCATION
CREATE OR REPLACE TABLE customer_order_summary_external
USING DELTA
LOCATION 's3://cts-amzn-s3-db-bucket/db_work/customer_order_summary_external'
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
WHERE c.region IN ('US', 'EU', 'APAC');

-- 8) View outputs
SELECT * FROM customer_order_summary_managed;
SELECT * FROM customer_order_summary_external;

-- 9) Metadata check
DESCRIBE TABLE EXTENDED customer_order_summary_managed;
DESCRIBE TABLE EXTENDED customer_order_summary_external;

-- 10) Optimization examples in SQL
-- Broadcast join is usually done in PySpark; not typically in SQL directly.
-- Some other optimization steps in Databricks SQL:
-- OPTIMIZE customer_order_summary_managed ZORDER BY (customer_id, order_date);
-- ALTER TABLE customer_order_summary_managed SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
-- ALTER TABLE customer_order_summary_managed SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true');
