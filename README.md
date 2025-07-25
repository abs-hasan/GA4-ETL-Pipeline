![Python](https://img.shields.io/badge/Python-3.9+-blue)
![Airflow](https://img.shields.io/badge/Airflow-ETL-green)
![GA4](https://img.shields.io/badge/GA4-Analytics-yellow)
![SQL%20Server](https://img.shields.io/badge/SQL-Server-informational)


# 📊 GA4 Channel Cost ETL Pipeline (Python + SQL Server + Airflow)

A **production-ready ETL pipeline** that extracts daily **channel-level campaign data** from **Google Analytics 4 (GA4)**, transforms it using **Python (Pandas)**, and loads it into **SQL Server** — ready for analysis, dashboards, and business decisions.

## 🚀 Project Overview
This project showcases my **ETL (Extract, Transform, Load)** skills by building a pipeline that processes Google Analytics 4 (GA4) campaign data for business analysis. I extract data from GA4 using API requests, transform it with Python, and load the results into SQL Server for reporting and dashboard use. This pipeline streamlines the process of preparing GA4 data, enabling stakeholders to analyze campaign performance efficiently.


## 🎯 Business Use Case

In my role as a Data Scientist, I noticed our team was paying for a third-party tool to export GA4 data. It was:
- 💰 Costly (monthly subscription)
- 🕒 Time-consuming
- ⚠️ Prone to human error

I built this pipeline to replace that tool — saving money, reducing overhead, and providing **clean, analysis-ready data** for channel performance reporting.



## 🧩 What This Pipeline Does

### 🔄 Extract

- Authenticates and queries the **GA4 Reporting API**
- Pulls metrics over the **last 10 days**
- Metrics: `totalUsers`, `advertiserAdCost`
- Dimensions: `date`, `firstUserDefaultChannelGroup`

---

### 🛠️ Transform

- Renames columns to business-friendly names
- Converts `date`, `cost`, and `user` fields to proper types
- Computes:
  - `CostPercentage` → share of daily ad cost per channel
  - `UserPercentage` → share of daily users per channel

---

### 💾 Load

- **Upserts** data into SQL Server:
  - Checks if the row exists (by date & channel)
  - Updates if found, inserts if new

---


## 📊 Example Output
The pipeline produces a dataset with the following structure, ready for use in dashboards or reports:

| Date       | Channel                 | Users | Cost   | Cost % | Users % |
|------------|--------------------------|-------|--------|--------|---------|
| 2025-04-01 | Organic Search           | 1500  | 500.00 | 25.00  | 30.00   |
| 2025-04-01 | Paid Search              | 2000  | 1000.0 | 50.00  | 40.00   |
| 2025-04-01 | Direct                   | 1000  | 500.00 | 25.00  | 30.00   |

---

## Why It’s Useful
- **Time-Saving Automation**: Eliminates manual data collection, saving hours of work each week.
- **Accurate Insights**: Provides clean, transformed data for reporting and analysis.
- **Scalable Solution**: Can be integrated into larger workflows or scheduled for regular updates.


## 🧰 Tech Stack

- **Python** – ETL logic and transformation
- **Google Analytics Data API (GA4)** – Source data
- **Pandas** – Data wrangling and feature engineering
- **SQL Server** – Target data store (upsert strategy)
- **Airflow (optional)** – Orchestration and scheduling

---


## Setup and Usage
1. **Install Dependencies**:
   - Ensure you have Python installed.
   - Install required packages: `pip install pandas google-analytics-data`.

2. **Configure Connections**:
   - Set up GA4 API access by replacing the property ID in `ga4_etl_pipeline.py` (line 29) with your own: `"property": "properties/[YOUR_PROPERTY_ID]"`.
   - Configure SQL Server connection details in your environment (not included in this script).

3. **Run the Script**:
   - Execute the script: `python ga4_etl_pipeline.py`.
   - The script will extract GA4 data, transform it, and load it into SQL Server.

## Files
- `ga4_etl_pipeline.py`: The Python script that runs the ETL pipeline.

