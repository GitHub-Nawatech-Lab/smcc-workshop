# PostgreSQL to Superset DAG

This project contains an Apache Airflow DAG that extracts data from PostgreSQL, cleans it, stores it in HDFS, and creates an external Hive table for Superset visualization.

## Prerequisites

- Apache Airflow
- PostgreSQL
- HDFS
- Hive
- Python 3.11

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/GitHub-Nawatech-Lab/smcc-workshop.git
    cd smcc-workshop
    ```

2. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```

## DAG Overview

The DAG performs the following tasks:

1. **Extracts data from PostgreSQL**: Uses a [PythonOperator](http://_vscodecontentref_/1) to run a Python function that extracts data from PostgreSQL, drops missing values, and saves the cleaned data to a CSV file.
2. **Uploads cleaned data to HDFS**: Uses a [BashOperator](http://_vscodecontentref_/2) to upload the cleaned CSV file to HDFS.
3. **Creates an external Hive table**: Uses a [HiveOperator](http://_vscodecontentref_/3) to create an external Hive table for Superset visualization.
4. **Loads data from HDFS into the Hive table**: Uses a [HiveOperator](http://_vscodecontentref_/4) to load the data from HDFS into the Hive table.
5. **Cleans up temporary files**: Uses a [BashOperator](http://_vscodecontentref_/5) to remove the temporary CSV file.

## DAG Configuration

The DAG is defined in [dag_airflow.py](http://_vscodecontentref_/6). It is scheduled to run daily starting from January 1, 2024.

### Connections

- **PostgreSQL**: Connection ID `smcc_postgres`
- **Hive**: Connection ID `smcc_hive`

## Running the DAG

1. Start the Airflow web server and scheduler:
    ```sh
    airflow webserver
    airflow scheduler
    ```

2. Access the Airflow web UI and trigger the `postgres_to_superset` DAG.

## License

This project is licensed under the MIT License. See the [LICENSE](http://_vscodecontentref_/7) file for details.