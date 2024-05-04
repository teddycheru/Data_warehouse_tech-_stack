import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define your DAG
dag = DAG(
    'load_data_to_postgres_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Load data from CSV to PostgreSQL',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def load_data_to_postgres():
    try:
        # Connect to postgres DB
        conn = psycopg2.connect(
            dbname="data_warehouse",
            user="postgres",
            password="12345rdx",
            host="postgres", # The service name in docker-compose is 'postgres'
            port="5439"
        )
        # Open a cursor to perform database operations
        cur = conn.cursor()

        # SQL command to load data from CSV file into the vehicles_data table
        # Update the path to your CSV file accordingly
        sql_command = """
            COPY vehicles_data(id, make, model, year, mileage)
            FROM '/data/data.csv'
            DELIMITER ','
            CSV HEADER;
        """

        # Execute the SQL command
        cur.execute(sql_command)

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cur.close()
        conn.close()

        print("Data loaded successfully.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# Define the task that calls the load_data_to_postgres function
load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag,
)

# If you have other tasks in your DAG, set the dependencies here
# For example, if 't1' is the ID of a previous task in your DAG
# t1 >> load_data_task