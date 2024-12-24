from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

def load_products():
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="datamart",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    query = """
        insert into products (
            product_id,
            product_name,
            category,
            price
        )
        select
            product_id,
            product_name,
            category,
            price
        from product_history
        where valid_to is null
    """
    
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id="products",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    load_task = PythonOperator(
        task_id="load",
        python_callable=load_products
    )
