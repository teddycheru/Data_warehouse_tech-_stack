from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define your DAG
dag = DAG(
    'divide_data_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Divide data into 2 parts and save to database',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Define the task that calls the divide_data function
divide_data_task = PythonOperator(
    task_id='divide_data',
    python_callable=divide_data,
    dag=dag,
)