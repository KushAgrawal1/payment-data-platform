# airflow/dags/payment_pipeline_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Import our modular helpers
from data_generator import generate_payments
from gold_processor import load_gold_layer_to_dw, run_data_quality

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

dag = DAG(
    'payment_data_pipeline',
    default_args=default_args,
    description='Daily Payment Data Lakehouse Orchestration Pipeline',
    schedule_interval='0 2 * * *',   # Daily at 2 AM
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1
)

# 1. Create Kafka Topics (if missing)
create_topics = BashOperator(
    task_id='create_kafka_topics',
    bash_command='python /opt/airflow/dags/kafka_admin.py',
    dag=dag,
)

# 2. Populate Pipeline with Fresh Real-Time Data Mocking
generate_data = PythonOperator(
    task_id='generate_payment_data',
    python_callable=generate_payments,
    op_kwargs={'num_records': 150},
    dag=dag,
)

# 3. Load Gold Layer aggregates into PostgreSQL
load_to_postgres = PythonOperator(
    task_id='load_gold_to_postgres',
    python_callable=load_gold_layer_to_dw,
    dag=dag,
)

# 4. Perform Data Quality checks on final analytical datasets
data_quality_check = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality,
    dag=dag,
)

# Define Dependency Tree
create_topics >> generate_data >> load_to_postgres >> data_quality_check