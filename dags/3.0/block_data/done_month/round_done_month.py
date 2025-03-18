import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

os.environ["DBT_LOG_PATH"] = "/tmp/dbt_logs"

block_category = 'done_month'
block_name = 'round_done_month'


default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    f'data-warehouse-test-dbt-{block_name}',
    default_args=default_args,
    description=f'Run {block_name} dbt model',
    schedule='0 16 * * *',
    tags=["3.0", "block", block_category]
)

dbt_run_student_indicator = BashOperator(
    task_id=f'dbt_run_{block_name}',
    bash_command='dbt run --profiles-dir /opt/airflow/dags/repo/dbt/dbt_dev_3/.dbt --project-dir /opt/airflow/dags/repo/dbt/dbt_dev_3 --model --select round_DM.sql',
    dag=dag
)

 
