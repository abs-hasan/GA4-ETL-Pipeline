# Google Analytics4 ETL Pipeline

## Overview
This project showcases my **ETL (Extract, Transform, Load)** skills by building a pipeline that processes Google Analytics 4 (GA4) campaign data for business analysis. I extract data from GA4 using API requests, transform it with Python, and load the results into SQL Server for reporting and dashboard use. This pipeline streamlines the process of preparing GA4 data, enabling stakeholders to analyze campaign performance efficiently.

## Motivation
In my role as a data scientist, I identified the need to automate the collection and processing of GA4 campaign data, which was previously done using a third-party tool. That tool required a monthly subscription fee and was time-consuming to manage. I built this pipeline to eliminate the subscription cost, save time, and reduce errors, providing a reliable data source for marketing analytics and supporting better decision-making for campaign optimization.

## What It Does
- **Extract**: Pulls campaign data (e.g., users, ad costs) from GA4 using API requests over a 10-day period.
- **Transform**: Processes the data with Python (pandas) by:
  - Converting data types (e.g., dates, numeric values).
  - Calculating daily channel percentages for costs and users.
  - Renaming columns for consistency.
- **Load**: Upserts the transformed data into SQL Server, ensuring existing records are updated and new ones are inserted.

## Why It’s Useful
- **Time-Saving Automation**: Eliminates manual data collection, saving hours of work each week.
- **Accurate Insights**: Provides clean, transformed data for reporting and analysis.
- **Scalable Solution**: Can be integrated into larger workflows or scheduled for regular updates.

## Sample Output
The pipeline produces a dataset with the following structure, ready for use in dashboards or reports:

| Date       | FirstUserDefaultChannelGroup | TotalUsers | AdvertiserAdCost | CostPercentage | UserPercentage |
|------------|------------------------------|------------|------------------|----------------|----------------|
| 2025-04-01 | Organic Search               | 1500       | 500.00           | 25.0           | 30.0           |
| 2025-04-01 | Paid Search                  | 2000       | 1000.00          | 50.0           | 40.0           |
| 2025-04-01 | Direct                       | 1000       | 500.00           | 25.0           | 20.0           |

## Tools Used
- **Python**: For data extraction and transformation (pandas).
- **GA4 API**: To extract campaign data.
- **SQL Server**: To store the processed data.

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

