import os
import csv
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from psycopg2 import sql
from contextlib import closing

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'northwind_etl',
    default_args=default_args,
    description='ETL pipeline for Northwind data',
    schedule_interval=timedelta(days=1),
)

def extract_data_from_postgres(**kwargs):
    execution_date = kwargs['ds']
    # Substitua 'northwind_db' pelo nome da conexão configurada no Airflow
    postgres_hook = PostgresHook(postgres_conn_id='postgres_northwind')
    conn_string = postgres_hook.get_uri()
    target_dir = os.path.join(kwargs['AIRFLOW_HOME'], f"data/postgres/{execution_date}")
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    tables = ['categories', 'customers', 'employees', 'orders', 'products', 'shippers', 'suppliers']
    with closing(psycopg2.connect(conn_string)) as conn:
        with conn.cursor() as cur:
            for table in tables:
                query = sql.SQL("COPY (SELECT * FROM {}) TO STDOUT WITH CSV HEADER").format(sql.Identifier(table))
                file_path = os.path.join(target_dir, f"{table}.csv")
                with open(file_path, 'w', newline='') as file:
                    cur.copy_expert(query, file)

def extract_data_from_csv(**kwargs):
    execution_date = kwargs['ds']
    source_file = os.path.join(kwargs['AIRFLOW_HOME'], 'data/order_details.csv')
    target_dir = os.path.join(kwargs['AIRFLOW_HOME'], f"data/csv/{execution_date}")
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    target_file = os.path.join(target_dir, 'order_details.csv')

    with open(source_file, 'r') as src, open(target_file, 'w', newline='') as dest:
        reader = csv.reader(src)
        writer = csv.writer(dest)
        for row in reader:
            writer.writerow(row)

def load_table(conn, file_path, table):
    with open(file_path, 'r') as f:
        cursor = conn.cursor()
        cursor.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER", f)
        conn.commit()
        cursor.close()

def load_data_to_postgres(**kwargs):
    execution_date = kwargs['ds']
    postgres_hook = PostgresHook(postgres_conn_id='postgres_northwind')
    conn = postgres_hook.get_conn()
    
    # Carregar dados para cada tabela
    tables = ['categories', 'customers', 'employees', 'orders', 'products', 'shippers', 'suppliers']
    for table in tables:
        file_path = os.path.join(kwargs['AIRFLOW_HOME'], f"data/postgres/{execution_date}/{table}.csv")
        load_table(conn, file_path, table)

    # Carregar order_details
    order_details_path = os.path.join(kwargs['AIRFLOW_HOME'], f"data/csv/{execution_date}/order_details.csv")
    load_table(conn, order_details_path, 'order_details')

    conn.close()

extract_postgres_task = PythonOperator(
    task_id='extract_postgres_data',
    python_callable=extract_data_from_postgres,
    op_kwargs={'execution_date': '{{ ds }}'},
    dag=dag,
)

extract_csv_task = PythonOperator(
    task_id='extract_csv_data',
    python_callable=extract_data_from_csv,
    op_kwargs={'execution_date': '{{ ds }}'},
    dag=dag,
)

load_postgres_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    op_kwargs={'execution_date': '{{ ds }}'},
    dag=dag,
)

extract_postgres_task >> extract_csv_task >> load_postgres_task