# Olist E-commerce Data Pipeline — Medallion Architecture
 
**Dataset**: [Brazilian E-commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Kaggle)  
**Tools Used**: Databricks · PySpark · Spark SQL · Delta Lake

---

## Overview

This project implements a three-tier Medallion Architecture data pipeline on the Olist Brazilian e-commerce dataset, a real-world dataset covering approximately 100,000 orders placed across Brazil between 2016 and 2018. The pipeline progresses data through three layers of increasing quality and analytical value, from raw CSV ingestion to business-ready star schema tables suitable for direct consumption by BI tools and dashboards.

---

## Dataset

The Olist dataset is a publicly available Brazilian e-commerce dataset published on Kaggle. It consists of 9 relational tables covering the full order lifecycle on the Olist marketplace platform, where independent sellers list products and fulfil orders directly to customers across Brazil. The dataset includes order transactions, customer and seller geolocation, product catalogue with physical dimensions, payment records, and customer reviews.

---

## Architecture

Bronze (Raw Ingestion) -> Silver (Cleaned & Validated) -> Gold (Business-Ready Analytics)

### Bronze Layer — `olist (bronze).ipynb`

Ingests all 9 raw CSV files from a Databricks Unity Catalog Volume into Delta tables. No transformations are applied. An `ingestion_timestamp` audit column is added to each table to record load time. All 9 tables are ingested in a single dictionary-driven loop for maintainability. A post-ingestion validation cell confirms successful writes and reports row counts for each table.

### Silver Layer — `olist (silver).ipynb`

Cleans, validates, and enriches each bronze table independently before writing it to a corresponding silver Delta table. Key transformations include:

- City name standardisation across customers, sellers, and geolocation
- Coordinate validation and ZIP-level deduplication for the geolocation table
- Review score extraction from a mixed-type field containing both valid scores and erroneous timestamp strings
- Payment record cleaning and type standardisation
- Order status consolidation and delivery performance flagging
- Product category translation and macro-category consolidation from 73 granular values to 10 business-friendly groups
- Physical dimension validation with weight unit conversion

All 6 critical foreign key relationships are validated via left anti joins after all tables are written. Payment totals are reconciled against order item totals at the order level with a 1-cent rounding tolerance.

### Gold Layer — `olist (gold).ipynb`

Transforms silver tables into a star schema model optimised for business analytics. Includes:

- **2 fact tables** at the order item and order grain
- **4 dimension tables** covering customers, products, sellers, and a date calendar
- **6 aggregate tables** covering daily sales, monthly sales with month-over-month metrics, product performance, seller rankings, customer lifetime value, and regional sales by state and macro-region

All aggregate tables source exclusively from gold fact and dimension tables. Each table is followed by a dedicated row count and integrity validation cell.

---

## Repository Structure

```
olist-medallion-pipeline/
├── olist (bronze).ipynb        # Raw ingestion from CSV to Delta
├── olist (silver).ipynb        # Cleaning, validation, and enrichment
├── olist (gold).ipynb          # Star schema and business aggregates
└── README.md
```

---

## Key Technical Features

### Data Quality
- Primary key uniqueness validated on all 9 tables
- Foreign key referential integrity validated across all 6 relationships
- Range validations on zip codes (Brazilian format 1000–99999), geographic coordinates (Brazilian boundaries), prices, payment values, and product dimensions
- State codes validated against all 27 official Brazilian state codes
- Payment reconciliation between order item totals and recorded payment amounts
- Row count and business rule validation cells after every gold table creation

### Transformations
- Custom PySpark UDF for Brazilian city name standardisation (accent removal, apostrophe handling, etc.) with a curated municipality consolidation dictionary
- Mixed-type review score field cleaned to extract valid integer scores (1–5), with malformed rows filtered
- Geolocation table deduplicated from 1,000,163 rows to one representative coordinate per ZIP prefix using coordinate averaging
- Product categories translated from Portuguese and consolidated from 73 granular values into 10 business-friendly macro-categories
- Order status consolidated and enriched with delivery performance flags: Early, On Time, Late, In-Progress, Canceled, Unavailable
- Customer and seller records enriched with Brazilian macro-region assignments: North, Northeast, Central-West, Southeast, South

### Gold Layer Design
- Star schema with fact tables at the order item and order grain
- Generated date dimension spanning 2016–2028 with Brazilian public holiday flags
- Pre-aggregated tables at daily, monthly, product, seller, customer, and regional grain
- Month-over-month revenue and order volume metrics using window functions
- Customer lifetime value and RFM components (recency, frequency, monetary) in the customer lifespan aggregate
- Seller concentration analysis via cumulative revenue share window function
- All gold tables read exclusively from gold facts and dimensions, not silver

---

## Data Quality Findings

Several noteworthy data quality issues were identified and resolved during silver layer processing.

**Review scores**: The `review_score` field contained mixed data — valid integer scores alongside timestamp strings entered into the wrong field. A cleaned column was derived by extracting only values in the range 1–5, with malformed rows removed.

**Geolocation**: The raw table contained coordinates outside Brazilian geographic boundaries, which were dropped before ZIP-level averaging was applied. The raw table also contained over 1 million rows due to multiple coordinate readings per ZIP prefix, reduced to one representative row per ZIP through averaging.

**Payments**: Eleven payment records had either a zero payment value or a missing instalment count. Zero-value payments were removed as commercially meaningless. Positive-value payments missing an instalment count were defaulted to 1.

**Orders without items or payments**: 776 orders existed in the orders table with no corresponding item or payment records, representing orders cancelled before any transaction was processed. These are intentionally excluded from `gold_fact_orders` via inner join and documented in the table's validation cell.

---

## How to Run

1. Download the 9 Olist CSV files from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
2. Upload the CSV files to a Databricks Unity Catalog Volume
3. Update the `base_path` variable in the bronze notebook to match your workspace, catalog, schema, and volume path
4. Run `olist (bronze).ipynb` to ingest all source files
5. Run `olist (silver).ipynb` to clean and validate all tables
6. Run `olist (gold).ipynb` to build the star schema and aggregates

Each notebook is designed to run top to bottom in a single execution. All notebooks use overwrite mode and re-running will reload and reprocess cleanly without duplication.

---

## Requirements

| Requirement | Detail |
|---|---|
| Databricks workspace | Unity Catalog enabled |
| Databricks Runtime | 13.0 or later (PySpark 3.4+) |
| Delta Lake | Included in Databricks Runtime |
| Source data | 9 Olist CSV files loaded to a Databricks Volume |

---

## Gold Layer — Table Reference

| Table | Grain | Description |
|---|---|---|
| `gold_fact_order_items` | One row per order line item | Item-level revenue, freight, and delivery metrics |
| `gold_fact_orders` | One row per order | Order-level revenue, payment, review, and delivery summary |
| `gold_dim_customers` | One row per customer_id | Customer geography and region |
| `gold_dim_products` | One row per product | Product catalogue with dimensions and macro-category |
| `gold_dim_sellers` | One row per seller | Seller geography and region |
| `gold_dim_dates` | One row per calendar date | Date intelligence: year, month, quarter, holidays, weekday flags |
| `gold_agg_daily_sales` | One row per day | Daily revenue, order volume, delivery, and review metrics |
| `gold_agg_monthly_sales` | One row per month | Monthly rollup with month-over-month change metrics |
| `gold_agg_product_performance` | One row per product | Revenue, units sold, delivery speed, and review scores by product |
| `gold_agg_top_sellers` | One row per seller | Seller rankings by revenue with cumulative Pareto share |
| `gold_agg_customer_lifespan` | One row per customer_unique_id | Lifetime value, order frequency, recency, and lifecycle segment |
| `gold_agg_regional_sales` | One row per region/state | Revenue, order volume, and delivery performance by geography |

---

## Data Source

Olist, the largest department store in Brazilian marketplaces, published this dataset on Kaggle for educational and research purposes.

**Dataset**: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

