from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pytz
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

def load_fact_sales(**kwargs):
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
        insert into fact_sales (
            order_id,
            customer_id,
            customer_name,
            product_id,
            product_name,
            quantity,
            price,
            transaction_type,
            store_id,
            order_date
        )
        
        with sales as (
            select
                order_id,
                customer_id,
                product_id,
                quantity,
                price,
                'ONLINE' as transaction_type,
                'ONLINE STORE' as store_id,
                order_date
            from online_sales

            union all

            select
                transaction_id as order_id,
                customer_id,
                product_id,
                quantity,
                price,
                'OFFLINE' as transaction_type,
                store_id,
                transaction_date as order_date
            from offline_sales
        )

        , breakdown as (
            select
                s.order_id,
                s.customer_id,
                c.name as customer_name,
                s.product_id,
                p.product_name,
                s.quantity,
                s.price,
                s.transaction_type,
                s.store_id,
                s.order_date
            from sales s
            left join customers c on
                s.customer_id = c.customer_id
            left join products p on
                s.product_id = p.product_id
        )

        select * from breakdown
    """

    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id="fact_sales",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    load_task = PythonOperator(
        task_id="load",
        python_callable=load_fact_sales
    )
