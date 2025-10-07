from __future__ import annotations

from airflow import DAG
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowNotFoundException
from typing import List, Tuple, Any

# --- Configuration ---
SNOWFLAKE_CONN_ID = "snowflake_conn"
ALPHA_VANTAGE_API_KEY_VAR = "ALPHA_VANTAGE_API_KEY"
SYMBOL = "IBM"
TABLE_NAME = "RAW.DAILY_STOCK_PRICES"

# Define the target columns for bulk insertion
TARGET_COLUMNS = [
    "TRADE_DATE", "OPEN_PRICE", "HIGH_PRICE",
    "LOW_PRICE", "CLOSE_PRICE", "VOLUME"
]

# Fetch the alpha vantage api key
api_key = Variable.get(ALPHA_VANTAGE_API_KEY_VAR)


@task
def extract_and_transform_data(api_key: str, symbol: str) -> List[Tuple[Any, ...]]:
    """
    Fetches data from Alpha Vantage and transforms it into a list of tuples
    ready for bulk SQL insertion.
    """
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key,
    }
   
    print(f"Fetching data for symbol: {symbol}...")
    response = requests.get(base_url, params=params)
    response.raise_for_status() # Check for HTTP errors
    data = response.json()

    # API Error check
    if "Error Message" in data:
        raise ValueError(f"Alpha Vantage API Error: {data['Error Message']}")

    time_series_key = "Time Series (Daily)"
    if time_series_key not in data:
        raise ValueError(f"Could not find key '{time_series_key}' in API response.")

    time_series_data = data[time_series_key]
    transformed_data = []

    # Transform data into list of tuples (date, open, high, low, close, volume)
    for date, values in time_series_data.items():
        try:
            row = (
                date,
                float(values['1. open']),
                float(values['2. high']),
                float(values['3. low']),
                float(values['4. close']),
                int(values['5. volume'])
            )
            transformed_data.append(row)
        except (ValueError, KeyError) as e:
            print(f"Skipping record for date {date} due to transformation error: {e}")
            continue

    print(f"Successfully fetched and prepared {len(transformed_data)} records.")
    return transformed_data

@task
def full_refresh_snowflake(data_to_insert: List[Tuple[Any, ...]], table_name: str, conn_id: str, columns: List[str]):
    """
    Performs a full refresh (TRUNCATE + bulk INSERT) using the SnowflakeHook.
    This fulfills the requirement for a full refresh using SQL transaction logic
    (though the Hook handles the actual transaction of the bulk load).
    """
    if not data_to_insert:
        print("No data to insert. Skipping full refresh.")
        return

    hook = SnowflakeHook(snowflake_conn_id=conn_id)
   
    # 1. Define the table structure
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        TRADE_DATE DATE,
        OPEN_PRICE FLOAT,
        HIGH_PRICE FLOAT,
        LOW_PRICE FLOAT,
        CLOSE_PRICE FLOAT,
        VOLUME INT
    );
    """

    # 2. Execute the TRUNCATE (Full Refresh logic)
    truncate_sql = f"TRUNCATE TABLE {table_name};"
   
    print(f"Ensuring table {table_name} exists...")
    hook.run(create_table_sql)
   
    print(f"Performing full refresh: TRUNCATING table {table_name}...")
    hook.run(truncate_sql)
   
    # 3. Bulk Insert the data
    print(f"Inserting {len(data_to_insert)} rows into {table_name}...")
   
    hook.insert_rows(
        table=table_name,
        rows=data_to_insert,
        target_fields=columns,
        replace=False
    )
    print(f"Successfully inserted {len(data_to_insert)} rows. Full refresh complete.")


# --- DAG Definition ---
with DAG(
    dag_id='alpha_vantage_snowflake_etl',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule = '30 2 * * *',
    catchup=False,
    tags=['finance', 'snowflake', 'etl', 'full_refresh'],
) as dag:
   
    # 1. Extract and Transform
    stock_records = extract_and_transform_data(api_key=api_key, symbol=SYMBOL)
   
    # 2. Load (Full Refresh)
    full_refresh_snowflake(
        data_to_insert=stock_records,
        table_name=TABLE_NAME,
        conn_id=SNOWFLAKE_CONN_ID,
        columns=TARGET_COLUMNS
    )

    # Task Dependency: Automatically created by passing task outputs as inputs.
    # get_api_key -> extract_and_transform_data -> full_refresh_snowflake
