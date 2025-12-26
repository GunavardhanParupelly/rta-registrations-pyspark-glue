dashboardh link : https://analytics.zoho.in/open-view/497352000000020225


# 🚗 Vehicle Registration ETL & Data Warehouse (AWS Glue + PySpark)

## 📘 Project Overview
This project demonstrates a **production-grade ETL pipeline** built using **AWS Glue and PySpark**, designed to ingest, clean, and model vehicle registration data into a **star schema** optimized for analytics in **Amazon Redshift or Athena**.

The pipeline follows an industry-standard **Bronze → Silver → Gold** architecture:
- **ETL1** performs extensive data cleaning and staging.
- **ETL2** applies advanced data modeling, fuzzy matching, and dimension–fact construction.
- The **final DWH schema** supports scalable analytical queries and reporting dashboards.

---

## 🧩 Repository Structure

| File | Purpose |
|------|----------|
| `etl1_clean_and_stage.py` | **ETL1 (Raw → Stage):** Cleans raw vehicle registration data, fixes schema drift, normalizes fields (dates, maker, model, fuel), deduplicates by registration number, and writes clean Parquet files to S3. |
| `etl2_advclean_and_dimcreation.py` | **ETL2 (Stage → Gold):** Builds **dimension and fact tables** with surrogate key generation, fuzzy vehicle resolution, emission standard derivation, and adaptive file coalescing. Outputs a ready-to-load star schema layer. |
| `starschema.txt` | **Data Warehouse DDL:** SQL schema for the analytical layer, including dimensions (`vehicle`, `manufacturer`, `rta`, `date`) and the `fact_registrations` fact table with relational integrity. |

---

## 🏗️ End-to-End Architecture

```text
Raw CSVs in S3
     │
     ├──▶ ETL1: etl1_clean_and_stage.py
     │       - Cleans malformed data
     │       - Parses inconsistent date formats
     │       - Derives model, variant, and make year
     │       - Deduplicates and partitions by year/month
     │       - Outputs to: s3://.../stage_clean_source/
     │
     └──▶ ETL2: etl2_advclean_and_dimcreation.py
             - Builds dim_vehicle, dim_manufacturer, dim_rta
             - Generates surrogate keys using SHA2 hashing
             - Resolves fuzzy duplicates (Levenshtein distance)
             - Creates fact_registrations table
             - Dynamically coalesces files for Redshift efficiency
             - Outputs to: s3://.../gold_dim_* and /gold_fact_registrations/
