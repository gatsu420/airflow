from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pytz
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from cerberus import Validator

def load_customer_history():
    file_path = os.path.join(os.path.dirname(__file__), "customers.xlsx")
    df = pd.read_excel(file_path)
    df["registration_date"] = pd.to_datetime(df["registration_date"])

    # Validate df according to schema. WIll throw fatal error if
    # 1) At least one field is missing, or
    # 2) At least one field is null, or
    # 3) Wrong format (e.g., customer_id inputted as integer instead of string).
    schema = {
        "customer_id": {"type": "string"},
        "name": {"type": "string"},
        "email": {"type": "string"},
        "registration_date": {"type": "datetime"}
    }
    validator = Validator(schema)
    for i, row in df.iterrows():
        row_dict = row.to_dict()

        for k, v in row_dict.items():
            if not validator.validate({k: v}):
                print(f"row {i+1} field {k} has maslformed value '{v}': {validator.errors}")
                sys.exit(1)

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
        update customer_history
        set valid_to = %s
        where customer_id = %s
        and (
            name != %s
            or email != %s
        )
        and valid_to is null
    """
    insert_query = """
        insert into customer_history (
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
            from customer_history
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

def load_customers():
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="datamart",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    query = """
        insert into customers (
            customer_id,
            name,
            email,
            registration_date
        )
        select
            customer_id,
            name,
            email,
            registration_date
        from customer_history
        where valid_to is null
    """

    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id="customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    load_customer_history_task = PythonOperator(
        task_id="load_customer_history",
        python_callable=load_customer_history
    )
    load_customers_task = PythonOperator(
        task_id="load_customers",
        python_callable=load_customers
    )

    load_customer_history_task >> load_customers_task
