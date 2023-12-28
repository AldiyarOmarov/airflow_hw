import os
import sys
import datetime as dt
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from modules.pipeline import pipeline
from modules.predict import predict

path = os.path.expanduser('~/airflow_hw')

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 06 * * *",
        default_args=args,
) as dag:

    pipeline_task = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
        dag=dag,
    )

    predict_task = PythonOperator(
        task_id='predict',
        python_callable=predict,
        dag=dag,
    )

    # <YOUR_CODE>
    pipeline_task >> predict_task
