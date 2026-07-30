# Building a Medallion Architecture Pipeline with the Olist E-Commerce Dataset

**96K+ orders analysed** · **12 validated gold tables** · **7 interactive dashboards** · **~R$2.9M in recoverable revenue identified**

**Dataset**: [Brazilian E-commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

**Tools Used**: Databricks · PySpark · Spark SQL · Delta Lake · Tableau Public

---

## Table of Contents

## Table of Contents

- [Overview](#overview)
- [Skills Demonstrated](#skills-demonstrated)
- [Dataset](#dataset)
- [Architecture](#architecture)
  - [Bronze Layer](#bronze-layer--01_bronze_layeripynb)
  - [Silver Layer](#silver-layer--02_silver_layeripynb)
  - [Gold Layer](#gold-layer--03_gold_layeripynb)
  - [SQL Query Layer](#sql-query-layer--04_tableau_sql_queriesipynb)
  - [Visualisation Layer](#visualisation-layer--tableau-public-dashboards)
- [BI Report](#bi-report)
- [Key Insights & Strategic Recommendations](#key-insights--strategic-recommendations)
- [Data Quality Findings](#data-quality-findings)
- [Repository Structure](#repository-structure)
- [Key Technical Features](#key-technical-features)
- [How to Run](#how-to-run)
- [Requirements](#requirements)
- [Gold Layer — Table Reference](#gold-layer--table-reference)
- [Data Source](#data-source)
- [Author](#author)

---

## Overview

This project implements a three-tier Medallion Architecture data pipeline on the Olist Brazilian e-commerce dataset, a real-world dataset covering approximately 100,000 orders placed across Brazil between 2016 and 2018. The pipeline progresses data through three layers of increasing quality and analytical value, from raw CSV ingestion to business-ready star schema tables that are ready for direct consumption by BI tools and dashboards.

The gold layer tables feed a series of 7 Tableau Public dashboards, organised around four analytical pillars: Product Performance, Seller Performance, Regional Opportunities, and Customer Analytics. These form the basis of a comprehensive BI report summarising platform performance, risk areas, and strategic recommendations.

---

## Skills Demonstrated

- **Data Engineering**: Medallion Architecture (Bronze/Silver/Gold) design, PySpark ETL pipelines, Delta Lake, dictionary-driven ingestion patterns
- **Data Modeling**: Star schema design, fact/dimension separation, grain management, slowly-changing reference dimensions (date dimension with holiday flags)
- **Data Quality & Validation**: Primary/foreign key integrity checks, range validation, payment reconciliation, mixed-type field cleaning, documented edge-case handling
- **SQL**: Window functions, CTEs, subqueries, cross joins with zero-fill for time-series continuity
- **Analytics & Segmentation**: RFM (recency, frequency, monetary) segmentation, Pareto/concentration analysis, quartile-based performance tiering, value-sentiment quadrant classification
- **BI & Visualisation**: Tableau Public dashboard design, calculated fields, executive reporting, insight-to-recommendation translation, revenue impact modelling

---

## Dataset

The Olist dataset is a publicly available Brazilian e-commerce dataset published on Kaggle. It consists of 9 relational tables covering the full order lifecycle on the Olist marketplace platform, where independent sellers list products and fulfil orders directly to customers across Brazil. The dataset includes order transactions, customer and seller geolocation, product catalogue with physical dimensions, payment records, and customer reviews.

---

## Architecture

Bronze (Raw Ingestion) -> Silver (Cleaned & Validated) -> Gold (Business-Ready Analytics) -> SQL Query Layer (Dashboard-Specific Aggregation) -> Visualisation (Tableau Public) -> Reporting (BI Report)

### Bronze Layer — `01_bronze_layer.ipynb`

Ingests all 9 raw CSV files from a Databricks Unity Catalog Volume into Delta tables. No transformations are applied. An `ingestion_timestamp` audit column is added to each table to record load time. All 9 tables are ingested in a single dictionary-driven loop for maintainability. A post-ingestion validation cell confirms successful writes and reports row counts for each table.

### Silver Layer — `02_silver_layer.ipynb`

Cleans, validates, and enriches each bronze table independently before writing it to a corresponding silver Delta table. Key transformations include:

- City name standardisation across customers, sellers, and geolocation
- Coordinate validation and ZIP-level deduplication for the geolocation table
- Review score extraction from a mixed-type field containing both valid scores and erroneous timestamp strings
- Payment record cleaning and type standardisation
- Order status consolidation and delivery performance flagging
- Product category translation and macro-category consolidation from 73 granular values to 10 business-friendly groups
- Physical dimension validation with weight unit conversion

All 6 critical foreign key relationships are validated via left anti joins after all tables are written. Payment totals are reconciled against order item totals at the order level with a 1-cent rounding tolerance.

### Gold Layer — `03_gold_layer.ipynb`

Transforms silver tables into a star schema model optimised for business analytics. Includes:

- **2 fact tables** at the order item and order grain
- **4 dimension tables** covering customers, products, sellers, and a date calendar
- **6 aggregate tables** covering daily sales, monthly sales with month-over-month metrics, product performance, seller rankings, customer lifetime value, and regional sales by state and macro-region

All aggregate tables source exclusively from gold fact and dimension tables. Each table is followed by a dedicated row count and integrity validation cell.

### SQL Query Layer — `04_tableau_sql_queries.ipynb`

Each Tableau Public dashboard is backed by one or more dashboard-specific SQL queries that read exclusively from gold layer tables. These queries are documented in a dedicated notebook, organised into the same four analytical pillars used throughout this project, and contain the derivation logic behind the metrics and segments referenced in the BI report:

- **Product Performance Queries** — order status funnel, category-level freight ratio analysis, freight burden banding (Healthy <20% / Moderate 20–30% / High ≥30%), delivery speed banding, and the product-month cross join used to compute zero-filled rolling growth for pipeline risk product identification
- **Seller Performance Queries** — seller-level revenue ranking with cumulative Pareto share, and NTILE(4) quartile scoring across revenue, on-time delivery, and satisfaction that produces the composite score behind the seller performance tiers (Star Seller, High Revenue/Poor Delivery, Low Revenue/High Satisfaction, Developing, Underperformer)
- **Regional Opportunities Queries** — state and regional revenue and delivery rollups, seller coverage by state, and on-time delivery gap versus the national average
- **Customer Analytics Queries** — an RFM segmentation model (recency, frequency, monetary) persisted to a `customer_rfm_segmentation` table, from which the value-sentiment quadrant classification and reactivation priority tiers are derived

Because these queries read only from gold tables and were run in the same Databricks environment as the bronze, silver, and gold notebooks, they provide a fully reproducible link between the validated data pipeline and every figure presented in the Tableau dashboards and BI report.

### Visualisation Layer — Tableau Public Dashboards

The gold layer's fact, dimension, and aggregate tables, together with the dashboard-specific queries above, were published to Tableau Public and used to build 7 interactive dashboards. Each dashboard reads exclusively from gold tables, ensuring every visualisation is grounded in validated, transformation-complete data. The dashboards are organised around four analytical pillars, each addressing a distinct business concern:

- **Product Performance** — What is being bought, where are the sales and fulfilment gaps, and which products are liabilities? Covers platform-wide sales performance, order status funnel, category revenue concentration, delivery time distribution, and freight burden analysis to flag products where shipping costs erode margin (pipeline risk products).
- **Seller Performance** — Who is fulfilling orders, which sellers are underperforming, and how concentrated is revenue dependency? Covers seller revenue concentration (Pareto analysis), platform growth trajectory (active sellers and order intensity), and a seller performance scorecard tiering sellers by revenue, delivery speed, and customer satisfaction.
- **Regional Opportunities** — Where is this activity happening and where should the platform invest next? Covers geographic revenue distribution by state and macro-region, delivery performance gaps by state, and seller coverage versus revenue opportunity to identify underserved, high-potential markets.
- **Customer Analytics** — Who is buying and who is at risk of leaving? Covers customer value-sentiment segmentation, individual customer positioning by lifetime value and review score, satisfaction score distribution, and an RFM-based reactivation priority framework identifying at-risk revenue by customer tier.

Together, the 7 dashboards provide the visual and analytical foundation for the BI report.

#### Dashboard Links
| Dashboard Name | Pillar | Tableau Public Link |
|---|---|---|
| Sales Performance & Fulfilment Analysis | Product Performance | [URL Link](https://public.tableau.com/app/profile/zach.tanner/viz/OlistSalesPerformanceFulfillmentAnalysis/Dashboard1) |
| Freight Burden Analysis | Product Performance | [URL Link](https://public.tableau.com/app/profile/zach.tanner/viz/OlistFreightBurdenAnalysis/Dashboard2) |
| Seller Revenue Concentration Analysis | Seller Performance | [URL Link](https://public.tableau.com/app/profile/zach.tanner/viz/OlistSellerRevenueConcentrationandPlatformGrowth/SellerSummaryDashboard) |
| Seller Performance Scorecard | Seller Performance | [URL Link](https://public.tableau.com/app/profile/zach.tanner/viz/OlistSellerPerformanceScorecard/SellerTierDashboard) |
| Regional Sales Landscape | Regional Opportunities | [URL Link](https://public.tableau.com/app/profile/zach.tanner/viz/OlistRegionalSalesLandscape/RegionalDashboard) |
| Customer Sentiment & Value Segmentation | Customer Analytics | [URL Link](https://public.tableau.com/app/profile/zach.tanner/viz/OlistCustomerSentimentandValueSegmentation/SentimentTierDashboard) |
| Customer Reactivation Prioritisation | Customer Analytics | [URL Link](https://public.tableau.com/app/profile/zach.tanner/viz/olistCustomerReactivationPriority/ReactivationDashboard) |

--- 

## BI Report

Findings from the Tableau dashboard analysis were synthesised into a standalone BI report: *Olist E-Commerce Platform: Comprehensive Data Analysis Report*. The report is structured around the same four analytical pillars (Product Performance, Seller Performance, Regional Opportunities, Customer Analytics) plus platform overview, methodology, and strategic recommendations. The report translates dashboard-level findings into an executive summary, a quantified priority matrix, and a revenue impact model. It also documents the underlying Medallion pipeline and data quality decisions described in this README.

---

## Key Insights & Strategic Recommendations

Synthesised from the 7 Tableau dashboards and the accompanying BI report, the findings below represent the platform's highest-priority risks and opportunities across all four analytical pillars.

### Product Performance
- **904 products (21.6% of the catalogue)** exceed the 30% freight-risk threshold, exposing **R$541K** in revenue to margin erosion. **24 products** meet a three-way risk criterion (high freight ratio, sustained demand decline, and minimum sales volume) and are priority candidates for delisting or repricing.
- **3 product categories** (Living & Home Appliances, Sports/Toys & Leisure, Electronics & Gaming) drive ~50% of total revenue — a concentration that leaves the platform exposed to category-specific demand shocks.
- **Recommendation:** Review the 24 dual-flagged products for delisting; introduce freight surcharges for high-price/high-freight items with absorbable margin; expand marketing into freight-efficient, high-growth categories (e.g. Health, Beauty & Personal Care).

### Seller Performance
- Just **136 sellers (4.4% of the active base)** generate **~50% of platform GMV** — a single point of failure if even a handful churn.
- The **High Revenue/Poor Delivery** tier (321 sellers) generates **48% of GMV** yet delivers 27% slower than Star Sellers, with satisfaction scores already at the platform average — a lagging indicator, not a stable one.
- **Recommendation:** Launch a formal retention programme for the top 136 sellers; engage High Revenue/Poor Delivery sellers with proactive SLA targets before satisfaction erodes further.

### Regional Opportunities
- The **Southeast generates 64.6%** of total revenue, with **São Paulo alone contributing 37.4%** — geographic concentration that exceeds even the region's population share.
- Northern and Northeastern states consistently underperform on delivery speed (Alagoas trails the national average by **14.6 percentage points**), while **Bahia** shows demand outpacing local seller supply.
- **Recommendation:** Launch targeted seller recruitment in Bahia and across the Northeast/North to close delivery gaps and capture latent demand.

### Customer Analytics
- **1,226 high-value customers** (lifetime spend >R$500) are dissatisfied, representing **R$1.14M** in exposed revenue — spending ~7x more per customer than the low-value segment.
- **15,699 recently-inactive, high-value customers** hold **R$4.8M** in at-stake lifetime value. Their satisfaction and delivery experience remain strong, indicating churn is *situational, not experiential* — a low-friction reactivation opportunity.
- **Recommendation:** Launch immediate personalised recovery outreach for dissatisfied high-value customers, and prioritise Tier 1 reactivation campaigns in SP, RJ, and MG, which account for 63% of at-stake revenue.

### Estimated Recoverable Revenue

| Initiative | Revenue at Stake | Recovery Assumption* | Recoverable Revenue* |
|---|---|---|---|
| Tier 1 customer reactivation | R$4,825K | 20% conversion | R$965K |
| High-value/at-risk customer recovery | R$1,141K | 35% recovery | R$399K |
| Pipeline risk product margin improvement | R$541K | 40% margin recovery | R$218K |
| Low-value/satisfied loyalty programme | R$8,883K | 15% uplift | R$1,332K |
| **Total** | | | **~R$2.9M** |

*Recovery assumptions are illustrative, industry-benchmark estimates rather than figures derived from Olist platform data; actual results depend on execution.

**Bottom line:** The platform's operational fundamentals are strong (97.8% delivery success, 4.1 average review score), but concentration risk in sellers, revenue, and geography runs beneath the numbers. Each risk is quantified above, and each is addressable.

---

## Data Quality Findings

Several noteworthy data quality issues were identified and resolved during silver layer processing.

**Review scores**: The `review_score` field contained mixed data — valid integer scores alongside timestamp strings entered into the wrong field. A cleaned column was derived by extracting only values in the range 1–5, with malformed rows removed.

**Geolocation**: The raw table contained coordinates outside Brazilian geographic boundaries, which were dropped before ZIP-level averaging was applied. The raw table also contained over 1 million rows due to multiple coordinate readings per ZIP prefix, reduced to one representative row per ZIP through averaging.

**Payments**: Eleven payment records had either a zero payment value or a missing instalment count. Zero-value payments were removed as commercially meaningless. Positive-value payments missing an instalment count were defaulted to 1.

**Orders without items or payments**: 776 orders existed in the orders table with no corresponding item or payment records, representing orders cancelled before any transaction was processed. These are intentionally excluded from `gold_fact_orders` via inner join and documented in the table's validation cell.

---

## Repository Structure

```
olist-medallion-pipeline/
├── 01_bronze_layer.ipynb                    # Raw ingestion from CSV to Delta
├── 02_silver_layer.ipynb                    # Cleaning, validation, and enrichment
├── 03_gold_layer.ipynb                      # Star schema and business aggregates
├── 04_tableau_sql_queries.ipynb             # Dashboard-specific SQL queries sourced from gold tables
├── 05_dashboard_presentation.pdf            # Slide deck with 7 Tableau Public dashboards (screenshots & links), key insights & recommendations
├── 06_bi_report.pdf                         # Comprehensive analysis report synthesising dashboard findings
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
- Month-over-month revenue and order volume metrics using CTEs and window functions
- Customer lifetime value and RFM components (recency, frequency, monetary) in the customer lifespan aggregate
- Seller concentration analysis via cumulative revenue share window function
- All gold tables read exclusively from gold facts and dimensions, not the silver layer

### Dashboard SQL Layer
- Seller performance tiering via `NTILE(4)` quartile scoring across revenue, on-time delivery, and satisfaction, combined into a single composite score
- Product-month cross join with zero-fill used to compute accurate rolling 3-month growth rates and avoid false growth signals for products with no sales in a given month
- Freight ratio and delivery speed banding logic (e.g., Healthy <20% / Moderate 20–30% / High ≥30% freight burden flags) applied at both the category and individual product level
- RFM segmentation persisted to an intermediate `customer_rfm_segmentation` table, driving value-sentiment quadrant classification and a 5-tier reactivation priority framework
- State-level delivery performance benchmarked against a computed national on-time delivery average

---

## How to Run

1. Download the 9 Olist CSV files from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
2. Upload the CSV files to a Databricks Unity Catalog Volume
3. Update the `base_path` variable in the bronze notebook to match your workspace, catalog, schema, and volume path
4. Run `01_bronze_layer.ipynb` to ingest all source files
5. Run `02_silver_layer.ipynb` to clean and validate all tables
6. Run `03_gold_layer.ipynb` to build the star schema and aggregates
7. Run `04_tableau_sql_queries.ipynb` to generate and validate the dashboard-specific query outputs (seller tiering, freight banding, RFM segmentation, and regional benchmarks)
8. Connect Tableau Public to the gold layer tables and dashboard-specific query outputs, and build or refresh the 7 dashboards across the Product Performance, Seller Performance, Regional Opportunities, and Customer Analytics pillars
9. Use dashboard insights to update the accompanying BI report

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

---

## Author

**Zachary Tanner**

Data Analyst | Business Intelligence | Analytics Engineering

- LinkedIn: https://www.linkedin.com/in/zachary-p-tanner/
- GitHub: https://github.com/ZacTanner
- Tableau Public: https://public.tableau.com/app/profile/zach.tanner/vizzes
- Peer Reviewed Publications: https://orcid.org/0009-0001-1776-4459
- Email: zach.tanner1@gmail.com
