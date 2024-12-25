from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pytz
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

def load_mart_sales_daily(**kwargs):
    # Add start_date and end_date to enable backfilling.
    tz = pytz.timezone("Asia/Jakarta")
    start_date = kwargs["dag_run"].conf.get("start_date")
    end_date = kwargs["dag_run"].conf.get("end_date")
    if not start_date:
        start_date = (datetime.now(tz) - timedelta(days=1)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now(tz).strftime("%Y-%m-%d")
    print("start_date: ", start_date)
    print("end_date: ", end_date)

    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="datamart",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    query = """
        insert into mart_sales_daily (
            order_date,
            product_id,
            product_name,
            customer_name,
            transaction_type,
            store_id,
            total_quantity,
            total_price_paid,
            total_order
        )

        select
            order_date,
            product_id,
            product_name,
            customer_name,
            transaction_type,
            store_id,
            sum(quantity) as total_quantity,
            sum(price) as total_price_paid,
            count(distinct order_id) as total_order
        from fact_sales
        group by 1, 2, 3, 4, 5, 6
    """

    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id="mart_sales",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    load_task = PythonOperator(
        task_id="load_daily",
        python_callable=load_mart_sales_daily
    )
