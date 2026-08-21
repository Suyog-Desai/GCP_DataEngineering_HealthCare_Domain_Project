from airflow import DAG
from datetime import timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator as BQInJob

#Define the variables
PROJECT_ID= "project-66ab2fa5-e082-4e6e-9c4"
REGION = "us-central1"
SQL_FILE_PATH_1 = "/home/airflow/gcs/data/BQ/Bronze_external_table_query.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/data/BQ/Silver_truncate_insert_query.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/data/BQ/silver_incremental_merge_changes.sql"
SQL_FILE_PATH_4 = "/home/airflow/gcs/data/BQ/gold_tables_query.sql"

#Read SQL query from file

def read_sql_file(file_name):
    with open (file_name, "r") as file:
        return file.read()

BRONZE_QUERY = read_sql_file (SQL_FILE_PATH_1)
SILVER_QUERY_1 = read_sql_file (SQL_FILE_PATH_2)
SILVER_QUERY_2 = read_sql_file (SQL_FILE_PATH_3)
GOLD_QUERY = read_sql_file (SQL_FILE_PATH_4)

# Define Default Arguments

ARGS = {
    "owner": "Suyog Desai",
    "start_date": None,
    "depends_on_past": True,
    "email_on_failure": True,
    "email_on_retry": True,
    "email": ["suyogdesaiaws@gmail.com"],
    "email_on_success": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

#Define the DAG

with DAG (
    dag_id = "bigquery_dag",
    schedule_interval = None,
    description = "DAG to run the bigquery jobs",
    default_args = ARGS,
    tags = ["gcs", "bq","etl"]
) as dag:

    #task to create bronze tables
    bronze_tables = BQInJob(
        task_id = "bronze_tables",
        configuration = {
            "query":{
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority":"BATCH"
            }
        }
    )

    #task to create silver_tables
    silver_tables_1 = BQInJob (
        task_id = "silver_tables_1",
        configuration = {
            "query":{
                "query":SILVER_QUERY_1,
                "useLegacySql": False,
                "priority": "BATCH"
            }
        }
    )

    silver_tables_2 = BQInJob (
            task_id = "silver_tables_2",
            configuration = {
                "query":{
                    "query":SILVER_QUERY_2,
                    "useLegacySql": False,
                    "priority": "BATCH"
                }
            }
        )
    # Task to create gold table
    gold_tables = BQInJob(
            task_id = "gold_tables",
            configuration = {
                "query":{
                    "query": GOLD_QUERY,
                    "useLegacySql": False,
                    "priority":"BATCH"
                }
            }
        )

#Define Dependencies

bronze_tables >> [silver_tables_1 , silver_tables_2] >> gold_tables