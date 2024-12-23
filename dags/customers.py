from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pytz
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

def load_customers():
    file_path = os.path.join(os.path.dirname(__file__), "customers.xlsx")
    df = pd.read_excel(file_path)
    print(df)

    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="datamart",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    update_query = """
        update customers
        set valid_to = %s
        where customer_id = %s
        and (
            name != %s
            or email != %s
        )
        and valid_to is null
    """
    insert_query = """
        insert into customers (
            customer_id,
            name,
            email,
            registration_date,
            valid_from,
            valid_to
        )
        select %s, %s, %s, %s, %s, null
        where not exists (
            select 'p'
            from customers
            where customer_id = %s
            and valid_to is null
        )
    """

    tz = pytz.timezone("Asia/Jakarta")
    current_timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S %z")
    
    update_data = [(
        current_timestamp, row.customer_id, row.name, row.email
    ) for row in df.itertuples()]
    insert_data = [(
        row.customer_id, row.name, row.email, row.registration_date, current_timestamp, row.customer_id
    ) for row in df.itertuples()]

    execute_batch(cursor, update_query, update_data)
    execute_batch(cursor, insert_query, insert_data)
    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id="customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    load_task = PythonOperator(
        task_id="load",
        python_callable=load_customers
    )