from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os



# CONFIG

BASE_URL = os.getenv("API_BASE_URL", "https://api.coingecko.com/api/v3")
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "/opt/airflow/data/raw")

API_CONFIG = {
    "coin_markets": {
        "endpoint": "/coins/markets",
        "params": {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 1000,
            "page": 1
        }
    },
    "coin_details": {
        "endpoint": "/coins/bitcoin",
        "params": {}
    },
    "simple_price": {
        "endpoint": "/simple/price",
        "params": {
            "ids": "bitcoin,ethereum",
            "vs_currencies": "usd"
        }
    }
}

# TASK 1: Extract
def extract_api(api_name, endpoint, params, **context):
    import requests

    url = f"{BASE_URL}{endpoint}"
    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    context['ti'].xcom_push(
        key=f"{api_name}_data",
        value=data
    )


# TASK 2: Save Raw
def save_raw(api_name, **context):
    import os
    import json

    ti = context['ti']
    execution_date = context['ds']

    data = ti.xcom_pull(key=f"{api_name}_data")

    path = f"{RAW_DATA_PATH}/{api_name}/{execution_date}"
    os.makedirs(path, exist_ok=True)

    file_path = f"{path}/data.json"

    with open(file_path, "w") as f:
        json.dump(data, f)

    print(f"[{api_name}] Data saved to {file_path}")


# TASK 3: Trigger Databricks (placeholder)
def trigger_databricks(**context):
    print("Triggering Databricks job...")
    # later: integrate with Databricks API

# DAG

default_args = {
    "owner": "adhiyana",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_dynamic_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["crypto", "databricks", "medallion"],
) as dag:

    for api_name, config in API_CONFIG.items():

        extract_task = PythonOperator(
            task_id=f"extract_{api_name}",
            python_callable=extract_api,
            op_kwargs={
                "api_name": api_name,
                "endpoint": config["endpoint"],
                "params": config["params"]
            }
        )

        save_task = PythonOperator(
            task_id=f"save_{api_name}",
            python_callable=save_raw,
            op_kwargs={
                "api_name": api_name
            }
        )

        extract_task >> save_task