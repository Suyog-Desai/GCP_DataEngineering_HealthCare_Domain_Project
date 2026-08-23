#import all modules
from airflow import DAG
from datetime import timedelta
from airflow.providers.google.cloud.operators.dataproc import ( 
    DataprocStartClusterOperator, DataprocStopClusterOperator, DataprocSubmitJobOperator
)

#define the variables

PROJECT_ID = "project-66ab2fa5-e082-4e6e-9c4"
REGION = "us-east4"
CLUSTER_NAME = "my-demo-cluster"
COMPOSER_BUCKET = "us-central1-demo-instance-3f1d41a0-bucket"

JOB_FILE_1_GCS = f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalA_mysqlToLanding.py"
PYSPARK_JOB_1 = {
    "reference":{"project_id":PROJECT_ID},
    "placement":{"cluster_name":CLUSTER_NAME},
    "pyspark_job":{"main_python_file_uri":JOB_FILE_1_GCS}
}

JOB_FILE_2_GCS = f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalB_mysqlToLanding.py"
PYSPARK_JOB_2 = {
    "reference":{"project_id":PROJECT_ID},
    "placement":{"cluster_name":CLUSTER_NAME},
    "pyspark_job":{"main_python_file_uri":JOB_FILE_2_GCS}
}

JOB_FILE_3_GCS = f"gs://{COMPOSER_BUCKET}/data/INGESTION/claims.py"
PYSPARK_JOB_3 = {
    "reference":{"project_id":PROJECT_ID},
    "placement":{"cluster_name":CLUSTER_NAME},
    "pyspark_job":{"main_python_file_uri":JOB_FILE_3_GCS}
}

JOB_FILE_4_GCS = f"gs://{COMPOSER_BUCKET}/data/INGESTION/cpt_codes.py"
PYSPARK_JOB_4 = {
    "reference":{"project_id":PROJECT_ID},
    "placement":{"cluster_name":CLUSTER_NAME},
    "pyspark_job":{"main_python_file_uri":JOB_FILE_4_GCS}
}


ARGS = {
    "owner":"Suyog Desai",
    "start_date":None,
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "email":["suyogdesaiaws@gmail.com"],
    "email_on_success":True,
    "retries":1,
    "retry_delay":timedelta(minutes=5)
}

#Define a DAG

with DAG(
    dag_id = "pyspark_dag",
    schedule_interval = None,
    description = "DAG to start a Dataproc cluster, run Pyspark jobs and stop the cluster",
    default_args = ARGS,
    tags = ["pyspark", "dataproc","etl"] 
) as dag:

    #define the tasks
    start_cluster = DataprocStartClusterOperator(
        task_id = "start_cluster",
        project_id = PROJECT_ID,
        region = REGION,
        cluster_name = CLUSTER_NAME
    )

    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id = "pyspark_task_1",
        job = PYSPARK_JOB_1,
        region = REGION,
        project_id = PROJECT_ID
    )

    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id = "pyspark_task_2",
        job = PYSPARK_JOB_2,
        region = REGION,
        project_id = PROJECT_ID
    )

    pyspark_task_3 = DataprocSubmitJobOperator(
        task_id = "pyspark_task_3",
        job = PYSPARK_JOB_3,
        region = REGION,
        project_id = PROJECT_ID
    )

    pyspark_task_4 = DataprocSubmitJobOperator(
        task_id = "pyspark_task_4",
        job = PYSPARK_JOB_4,
        region = REGION,
        project_id = PROJECT_ID
    )

    stop_cluster = DataprocStopClusterOperator (
        task_id = "stop_cluster",
        project_id = PROJECT_ID,
        region = REGION,
        cluster_name = CLUSTER_NAME
    )

#define the task dependencies

start_cluster >> pyspark_task_1 >> pyspark_task_2 >> pyspark_task_3 >> pyspark_task_4 >> stop_cluster

