from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python_operator import PythonOperator

# Note: These connection functions are used in the Airflow Task Automator project; replace with your own for SQL Server and GA4
from LookAfter.libs.airflow.hooks.sql_connection import sql_server_connection
from LookAfter.libs.airflow.hooks.ga4_connection import establish_ga4_connection

# Calculate the date range for data extraction
def calculate_time_range():
    # Set start time to current time plus 15 hours
    convert_time_start = datetime.now() + timedelta(hours=15)
    # Set end time to 240 hours before the start time
    convert_time_end = convert_time_start - timedelta(hours=240)
    return convert_time_start, convert_time_end

# Create a request payload for GA4 API
def request_for_ga4(date_str):
    return {
        "property": "properties/2234_____",
        "date_ranges": [{"start_date": date_str, "end_date": date_str}],
        "dimensions": [
            {"name": "date"},
            {"name": "firstUserDefaultChannelGroup"}
        ],
        "metrics": [
            {"name": "totalUsers"},
            {"name": "advertiserAdCost"}
        ]
    }

# Convert GA4 API response to a DataFrame
def create_dataframe(response):
    if not response.rows:
        return pd.DataFrame()
    
    data = [
        [v.value for v in list(row.dimension_values) + list(row.metric_values)]
        for row in response.rows
    ]
    columns = [h.name for h in list(response.dimension_headers) + list(response.metric_headers)]
    df = pd.DataFrame(data, columns=columns)
    return df

# Collect GA4 data for a date range
def loop_ga4_data(client, start_date, end_date):
    daily_dfs = []
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        request = request_for_ga4(date_str)
        response = client.run_report(request)
        daily_df = create_dataframe(response)
        daily_dfs.append(daily_df)
        current_date += timedelta(days=1)
    
    if daily_dfs:
        return pd.concat(daily_dfs, ignore_index=True)
    return pd.DataFrame()

# Convert DataFrame column data types
def change_datatypes(df):
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce')
    if 'TotalUsers' in df.columns:
        df['TotalUsers'] = pd.to_numeric(df['TotalUsers'], errors='coerce')
    if 'AdvertiserAdCost' in df.columns:
        df['AdvertiserAdCost'] = pd.to_numeric(df['AdvertiserAdCost'], errors='coerce')
    return df

# Calculate daily channel percentages for costs and users
def calculate_daily_channel_percentages(df):
    df['TotalCostByDay'] = df.groupby('Date')['AdvertiserAdCost'].transform('sum')
    df['TotalUsersByDay'] = df.groupby('Date')['TotalUsers'].transform('sum')
    df['CostPercentage'] = (df['AdvertiserAdCost'] / df['TotalCostByDay']) * 100
    df['UserPercentage'] = (df['TotalUsers'] / df['TotalUsersByDay']) * 100
    df.drop(columns=['TotalCostByDay', 'TotalUsersByDay'], inplace=True)
    return df

# Rename DataFrame columns for consistency
def column_renames(df):
    df.rename(columns={
        'date': 'Date',
        'firstUserDefaultChannelGroup': 'FirstUserDefaultChannelGroup',
        'totalUsers': 'TotalUsers',
        'advertiserAdCost': 'AdvertiserAdCost'
    }, inplace=True)
    return df

# Upsert data into SQL Server table
def upsert_data(df, cursor):
    for row in df.itertuples(index=False):
        cursor.execute('''
            SELECT COUNT(*) FROM dbo.ga4CostbyChannel
            WHERE Date = ? AND FirstUserDefaultChannelGroup = ?
        ''', (row.Date, row.FirstUserDefaultChannelGroup))

        if cursor.fetchone()[0] > 0:
            cursor.execute('''
                UPDATE dbo.ga4CostbyChannel
                SET TotalUsers = ?, AdvertiserAdCost = ?, CostPercentage = ?, UserPercentage = ?
                WHERE Date = ? AND FirstUserDefaultChannelGroup = ?
            ''', (row.TotalUsers, row.AdvertiserAdCost, row.CostPercentage, row.UserPercentage,
                  row.Date, row.FirstUserDefaultChannelGroup))
        else:
            cursor.execute('''
                INSERT INTO dbo.ga4CostbyChannel (Date, FirstUserDefaultChannelGroup, TotalUsers, AdvertiserAdCost, CostPercentage, UserPercentage)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (row.Date, row.FirstUserDefaultChannelGroup, row.TotalUsers, row.AdvertiserAdCost,
                  row.CostPercentage, row.UserPercentage))

# Main function to run the ETL pipeline
def run_ga4_channel_cost():
    # Establish connections to GA4 and SQL Server
    client = establish_ga4_connection()
    current_time, ten_days_ago = calculate_time_range()

    # Extract GA4 data
    raw_data_df = loop_ga4_data(client, ten_days_ago, current_time)
    if raw_data_df.empty:
        print("No GA4 data collected.")
        return

    # Transform data: rename columns, convert data types, calculate percentages
    renamed_df = column_renames(raw_data_df)
    change_dtypes = change_datatypes(renamed_df)
    final_df = calculate_daily_channel_percentages(change_dtypes)

    # Log the processed data
    print("Data to be upserted:")
    print(final_df.head())

    # Load data into SQL Server
    sql_conn, cursor = sql_server_connection()
    upsert_data(final_df, cursor)

    sql_conn.commit()
    cursor.close()
    sql_conn.close()
    print("Data upsert completed successfully.")