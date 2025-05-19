# 💼 End-to-End Banking Data Analysis Pipeline Project

This repository presents an end-to-end data engineering and analytics solution for a Czechoslovakia bank using **Snowflake**, **Power BI**, **AWS S3** and **MS Excel**. The goal is to ingest, store, clean, and analyze banking-related data to generate business insights and KPIs.

---

## 📊 Project Overview

The project simulates a modern data pipeline and analytics flow by performing the following:

- **Database & Table Creation** for all banking entities (clients, accounts, transactions, loans, etc.)
- **Ingestion** of CSV data from **AWS S3** using **Snowpipe**
- **Data Cleaning & Enrichment** using SQL in Snowflake
- **Business KPIs Calculation** (e.g., transactions summary, average balance, demographic insights)
- **Ad-Hoc Analysis** for executive-level insights
- **Power BI Visualization** for executive-level insights

---

## 🧱 Tech Stack

- **Cloud Data Warehouse:** Snowflake  
- **Cloud Storage:** AWS S3  
- **Data Ingestion:** Snowpipe (auto-ingest with SQS triggers)  
- **Scripting & Analytics:** SQL
- **Visualization:** Powe BI

---

## 🏗️ Project Architecture

```mermaid
graph TD
    A[CSV Data in S3 Bucket] -->|Auto-ingest via SQS| B[Snowpipe]
    B --> C[Snowflake Tables]
    C --> D[SQL Transformations and Data Cleaning]
    D --> E[Analytical Tables (KPIs)]
    E --> F[Ad-Hoc Insights and BI Reporting]

