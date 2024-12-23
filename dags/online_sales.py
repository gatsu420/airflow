from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pytz
import json
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

def load_online_sales(**kwargs):
    tz = pytz.timezone("Asia/Jakarta")
    start_date = kwargs["dag_run"].conf.get("start_date")
    end_date = kwargs["dag_run"].conf.get("end_date")
    if not start_date:
        start_date = (datetime.now(tz) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S %z")
    if not end_date:
        end_date = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S %z")
    print("start_date: ", start_date)
    print("end_date: ", end_date)

    file_path = os.path.join(os.path.dirname(__file__), "online_sales.json")
    with open(file_path, "r") as file:
        data = json.load(file)
    df = pd.DataFrame(data)
    df = df[(df["order_date"] >= start_date) & (df["order_date"] <= end_date)]
    df["created_at"] = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S %z")
    print(df)

    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="datamart",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    
    query = """
        insert into online_sales (
            order_id, 
            customer_id, 
            product_id, 
            quantity, 
            price, 
            order_date,
            created_at
        ) values %s
    """
    data = [row for row in df.itertuples(index=False, name=None)]
    
    execute_values(cursor, query, data)
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id="online_sales",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    load_task = PythonOperator(
        task_id="load",
        python_callable=load_online_sales,
    )
