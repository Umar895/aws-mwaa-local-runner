# Libraries
import json
import os
from datetime import datetime, timedelta
import pendulum
import pandas as pd
from pytrends.request import TrendReq

# Airflow
from airflow.models import DAG, Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.decorators import task

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
    BigQueryGetDatasetOperator,
    BigQueryGetDatasetTablesOperator,
    BigQueryUpdateDatasetOperator,
    BigQueryUpdateTableOperator,
    BigQueryUpdateTableSchemaOperator,
    BigQueryUpsertTableOperator,
)

LOCAL_TZ = pendulum.timezone("Europe/Berlin")

DAG_NAME = "Google Trends to Big Query" 
DAG_DESCRIPTION = "Designed to set for HD"
DAG_START_DATE = datetime(2022, 01, 16, tzinfo=LOCAL_TZ) 
DAG_SCHEDULE_INTERVAL = "@daily" # Cron notation -> see https://airflow.apache.org/scheduler.html#dag-runs
DAG_CATCHUP = False # When set to true, DAG will start running from DAG_START_DATE instead of current date
DAG_PAUSED_UPON_CREATION = True 
DAG_MAX_ACTIVE_RUNS = 5 

default_args = {
    "owner": "Airflow-HD",
    "start_date": DAG_START_DATE,
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2, # Max. number of retries before failing
    "retry_delay": timedelta(minutes=60) # Retry delay
}



def download_google_trend(ti):
    Today = datetime.datetime.today().strftime('%Y-%m-%d')
    number_of_years = 3
    date_in_past = (datetime.datetime.now() - datetime.timedelta(days=number_of_years*365)).strftime('%Y-%m-%d')

    timeline = f'{date_in_past} {Today}'
    keyword_list = ['Tom Hanks', 'Will Smith', 'Tom Cruise', 'angelina Jolie', 'Julia Roberts']

    pytrend = TrendReq(hl='en-US')

    pytrend.build_payload(kw_list=keyword_list, timeframe=timeline)

    df = pytrend.interest_over_time()

    ti.xcom_push(key='download_result', value=df)

    

# Create DAG
with DAG(
    DAG_NAME,
    description=DAG_DESCRIPTION,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    catchup=DAG_CATCHUP,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    is_paused_upon_creation=DAG_PAUSED_UPON_CREATION,
    default_args=default_args) as dag:

    # Start
    start = DummyOperator(
        task_id="start")

    extract_from_google = PythonOperator(task_id='download_file',
                                        python_callable=download_google_trend)


    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=PATH_TO_UPLOAD_FILE,
        dst=DESTINATION_FILE_LOCATION,
        bucket=BUCKET_NAME,
    )

    create_external_table = BigQueryCreateExternalTableOperator(
    task_id="create_external_table",
    table_resource={
        "tableReference": {
            "projectId": PROJECT_ID,
            "datasetId": DATASET_NAME,
            "tableId": "external_table",
        },
        "externalDataConfiguration": {
            "sourceFormat": "CSV",
            "compression": "NONE",
            "csvOptions": {"skipLeadingRows": 1},
            "sourceUris": [DATA_SAMPLE_GCS_URL],
        },
      },
    )

    get_dataset_info_bigQ = BigQueryGetDatasetOperator(task_id="get-dataset", dataset_id=DATASET_NAME)

    extract_from_google >> upload_file >> create_external_table >> get_dataset_info_bigQ
