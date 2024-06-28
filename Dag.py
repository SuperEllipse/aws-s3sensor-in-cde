
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'is_paused_upon_creation': False,    
}

# Define a simple Python function
def process_data():
    print("Processing data...")

# Define the DAG
with DAG(
    's3_sensor_pipeline',
    default_args=default_args,
    description='A simple S3 sensor DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 24),
    catchup=False,
) as dag:

    # # S3 Sensor task, un comment this code block and comment 
    # # the next block to check that the Airflow DAG is able to access the
    # # File location
    #
    # s3_sensor = S3KeySensor(
    #     task_id='s3_sensor',
    #     bucket_name='your-bucket-name-here',
    #     bucket_key='path-to-data/sample_data.csv',
    #     aws_conn_id='aws_default',
    #     poke_interval=30,
    #     timeout=600,
    # )
    
    # ensure that bucket_name and bucket_key are defined
    # as airflow variables before running this code block
    # S3 Sensor task
    s3_sensor = S3KeySensor(
        task_id='s3_sensor',
        aws_conn_id='aws_default',
        poke_interval=30,
        timeout=600,
        variables={'bucket_name': '{{ var.value.bucket_name}}', 
                   'bucket_key': '{{ var.value.bucket_key}}'
                   },
    )


    # BashOperator task
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "File found in S3"',
    )

    # PythonOperator task
    python_task = PythonOperator(
        task_id='python_task',
        python_callable=process_data,
    )

    # Define task dependencies
    s3_sensor >> bash_task >> python_task
