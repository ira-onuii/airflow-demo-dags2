import sys
import os

# 현재 파일이 있는 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import fst_lecture_query

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

user_filename = 'list.csv'




#user    
def fst_lecture_save_to_s3_with_hook(data, bucket_name, file_name):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)

    hook = S3Hook(aws_conn_id='conn_S3')
    hook.load_string(csv_buffer.getvalue(), key=f"{file_name}", bucket_name=bucket_name, replace=True)



def fst_lecture_save_results_to_s3(**context):
    query_results = context['ti'].xcom_pull(task_ids='fst_lecture_run_select_query')
    column_names = ["lecture_vt_No", "subject", "student_user_No", "student_name","teacher_user_No","teacher_name","rn","page_call_room_id"]
    df = pd.DataFrame(query_results, columns=column_names)
    fst_lecture_save_to_s3_with_hook(df, 'seoltab-datasource', user_filename)



default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'data-warehouse-test-tirno-AI',
    default_args=default_args,
    description='Run query and load result to S3',
    start_date=datetime(2024, 11, 13, 6, 30),
    schedule='*/25 * * * *',
    catchup=False
)



#user
fst_lecture_run_query = SQLExecuteQueryOperator(
    task_id='fst_lecture_run_select_query',
    sql=fst_lecture_query.fst_lecture_query,
    conn_id='trino_conn',
    do_xcom_push=True,
    dag=dag,
)


fst_lecture_save_to_s3_task = PythonOperator(
    task_id='fst_lecture_list_save_to_s3',
    python_callable=fst_lecture_save_results_to_s3,
    provide_context=True,
)






# dbt_run = BashOperator(
#     task_id='dbt_run',
#     bash_command='dbt run --profiles-dir /opt/airflow/dbt_project/.dbt --project-dir /opt/airflow/dbt_project --models /opt/airflow/dbt_project/models/pg_active_lecture/active_lecture.sql',
# )

fst_lecture_run_query >> fst_lecture_save_to_s3_task 


