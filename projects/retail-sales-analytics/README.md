# Retail Sales Analytics Project

This project analyzes retail sales data using Databricks notebooks. The workflow is structured in three main layers: Bronze, Silver, and Gold, following the Medallion architecture.

## Workflow Overview

1. **Catalog, Database and Volume Setup**
   - Created a Unity Catalog database (`retail_sales_catalog.sales`) and a volume to store raw data files.

2. **Bronze Layer: Ingest Raw Data**
   - Loaded orders data from CSV and customers data from JSON files in the bronze data folder.

3. **Silver Layer: Data Cleaning & Standardization**
   - Orders: Standardized order dates from multiple formats, cleaned column types, and stored as `silver_orders` table.
   - Customers: Cleaned sign-up dates, normalized column values, and stored as `silver_customers` table.

4. **Gold Layer: Enriched & Aggregated Analytics**
   - Joined orders and customers for enriched records.
   - Added temporal columns (year, month, date).
   - Aggregated analytics into the `golden_records` table for product/customer/time dimensions.

## Data Exploration

- Selected and visualized silver and gold layer tables for QA and analysis.
- Calculated key metrics/KPIs using aggregates from the pipeline, including total sales, orders, items sold, returned amounts, and customer counts.

## Main Notebook: nb-retail-sales

All steps above were implemented in `nb-retail-sales.ipynb` using both SQL and PySpark. Each transform and table creation is documented via code cell titles.

---

## Example Queries for Each Layer

**Bronze Layer: Raw Exploration**
```sql
SELECT * FROM retail_sales_catalog.sales.data.bronze.retail_dataset;
SELECT * FROM retail_sales_catalog.sales.data.bronze.customer_dataset;
```

**Silver Layer: Cleaned Data**
```sql
SELECT * FROM retail_sales_catalog.sales.silver_orders;
SELECT * FROM retail_sales_catalog.sales.silver_customers;
-- Filtered order data by status
SELECT * FROM retail_sales_catalog.sales.silver_orders WHERE order_status = 'completed';
-- Filtered customer data by loyalty tier
SELECT * FROM retail_sales_catalog.sales.silver_customers WHERE loyalty_tier = 'gold';
```

**Gold Layer: Aggregated Analytics**
```sql
SELECT category, SUM(total_sales) AS total_sales
FROM retail_sales_catalog.sales.golden_records
GROUP BY category;
-- Monthly sales trend
SELECT month, SUM(total_sales) AS monthly_sales
FROM retail_sales_catalog.sales.golden_records
GROUP BY month;
-- Customer-level sales aggregation
SELECT customer_id, SUM(total_sales) AS customer_sales
FROM retail_sales_catalog.sales.golden_records
GROUP BY customer_id;
-- Top products by quantity sold
SELECT product_name, SUM(total_quantity) AS quantity_sold
FROM retail_sales_catalog.sales.golden_records
GROUP BY product_name
ORDER BY quantity_sold DESC
LIMIT 10;
```
https://dbc-6173494c-a665.cloud.databricks.com/editor/files/891591767349005?o=7474658930637630$0

This README summarizes the current ETL flow and analytics performed for retail sales insights in Databricks.

