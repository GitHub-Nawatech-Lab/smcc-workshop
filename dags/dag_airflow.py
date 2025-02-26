from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.hive.operators.hive import HiveOperator
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

dag = DAG(
    "postgres_to_superset",
    description="DAG to extract data from PostgreSQL, clean it, store in HDFS, and create a Hive table for Superset.",
    start_date=datetime(2025, 2, 24),
    schedule_interval="@daily",
    tags=["smcc_demo"],
    catchup=False
)

dag.doc_md = """
### PostgreSQL to Superset DAG
This DAG performs the following tasks:
1. Extracts data from PostgreSQL
2. Cleans data by dropping missing values
3. Saves cleaned data to HDFS
4. Creates an external Hive table for Superset visualization

- **Start Date:** February 24, 2025  
- **Schedule:** Runs daily  
- **Connections Used:** PostgreSQL, HDFS, Hive
"""

extract_task = PythonOperator(
    task_id="extract_postgres",
    python_callable=extract_postgres,
    dag=dag
)

put_to_hdfs = BashOperator(
    task_id="upload_to_hdfs",
    bash_command="hdfs dfs -put -f /tmp/data_cleaned.csv /user/hive/warehouse/metastore.db/sales_data/",
    dag=dag
)

create_hive_table = HiveOperator(
    task_id="create_hive_table",
    hql="""
    CREATE EXTERNAL TABLE IF NOT EXISTS metastore.sales (
        salesordernumber STRING,
        salesorderlinenumber INT,
        orderdate STRING,
        customername STRING,
        emailaddress STRING,
        item STRING,
        quantity INT,
        unitprice FLOAT,
        taxamount FLOAT
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/metastore.db/sales_data/';
    """,
    hive_cli_conn_id="smcc_hive",
    dag=dag
)

# Step 4: Load data from HDFS into the Hive table
load_hdfs_to_hive = HiveOperator(
    task_id="load_hdfs_to_hive",
    hql="""
    LOAD DATA INPATH '/user/hive/warehouse/metastore.db/sales_data/data_cleaned.csv' 
    INTO TABLE metastore.sales_data;
    """,
    hive_cli_conn_id="smcc_hive",
    dag=dag
)

# Step 5: Cleanup temporary file
cleanup_task = BashOperator(
    task_id="cleanup_tmp_file",
    bash_command="rm -f /tmp/data_cleaned.csv",
    dag=dag
)

extract_task >> put_to_hdfs >> create_hive_table >> load_hdfs_to_hive >> cleanup_task