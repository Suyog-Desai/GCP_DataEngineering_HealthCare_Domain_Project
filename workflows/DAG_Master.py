from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import timedelta

#Define the default arguments

ARGS = {
    "onwer":"Suyog Desai",
    "start_date": days_ago(1),
    "email_on_past": True,
    "email_on_failure":True,
    "email_on_retry":True,
    "email":["suyogdesaiaws@gmail.com"],
    "email_on_success":True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

#Define the DAG

with DAG (
    dag_id = "master_dag",
    default_args = ARGS ,
    schedule_interval = "0 5 * * *",
    description = "Parent DAG to trigger PySpark and BigQuery DAGs",
    tags = ["parent", "orchestration", "etl"]
) as dag:

    #task to trigger pyspark job
    pyspark_task = TriggerDagRunOperator(
        task_id = "pyspark_task",
        trigger_dag_id = "DAG_Pyspark",
        wait_for_completion = True
    )

    #task to trigger bigquery job
    big_query_task = TriggerDagRunOperator(
        task_id = "big_query_task",
        trigger_dag_id = "DAG_BQ",
        wait_for_completion = True
    )

#Define Dependencies

pyspark_task >> big_query_task
    
