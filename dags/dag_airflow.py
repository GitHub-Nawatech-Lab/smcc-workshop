from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd

def extract_postgres():
    """Extracts data from PostgreSQL, drops missing values, and saves to CSV."""
    pg_hook = PostgresHook(postgres_conn_id="smcc_postgres")
    sql = "SELECT * FROM sales"
    df = pd.read_sql(sql, pg_hook.get_conn())
    df_cleaned = df.dropna()
    df_cleaned.to_csv('/tmp/data_cleaned.csv', index=False)

def load_to_clickhouse():
    """Loads cleaned data into ClickHouse using Airflow ClickHouseHook."""
    ch_hook = ClickHouseHook(clickhouse_conn_id="smcc_clickhouse")
    
    # Ensure the table exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sales (
        salesordernumber String,
        salesorderlinenumber Int32,
        orderdate String,
        customername String,
        emailaddress String,
        item String,
        quantity Int32,
        unitprice Float32,
        taxamount Float32
    ) ENGINE = MergeTree()
    ORDER BY salesordernumber;
    """
    ch_hook.run(create_table_query)

    # Read the cleaned data and insert it into ClickHouse
    df = pd.read_csv('/tmp/data_cleaned.csv')
    data = [tuple(row) for row in df.itertuples(index=False, name=None)]
    insert_query = "INSERT INTO sales VALUES"
    ch_hook.run(insert_query, parameters=data)

dag = DAG(
    "postgres_to_clickhouse",
    description="DAG to extract data from PostgreSQL, clean it, and insert it into ClickHouse.",
    start_date=datetime(2025, 2, 24),
    schedule_interval="@once",
    tags=["smcc_demo"],
    catchup=False
)

extract_task = PythonOperator(
    task_id="extract_postgres",
    python_callable=extract_postgres,
    dag=dag
)

load_clickhouse_task = PythonOperator(
    task_id="load_to_clickhouse",
    python_callable=load_to_clickhouse,
    dag=dag
)

cleanup_task = BashOperator(
    task_id="cleanup_tmp_file",
    bash_command="rm -f /tmp/data_cleaned.csv",
    dag=dag
)

extract_task >> load_clickhouse_task >> cleanup_task