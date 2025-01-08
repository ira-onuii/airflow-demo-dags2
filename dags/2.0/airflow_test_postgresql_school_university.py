import sys
import os

# 현재 파일이 있는 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import warehouse_query2

from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
#from airflow.operators.bash import BashOperator
import pandas as pd
from io import StringIO
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

user_filename = 'pg_school_university_'+date + '.csv'




#user    
def school_university_save_to_s3_with_hook(data, bucket_name, folder_name, file_name):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)

    hook = S3Hook(aws_conn_id='conn_S3')
    hook.load_string(csv_buffer.getvalue(), key=f"{folder_name}/{file_name}", bucket_name=bucket_name, replace=True)



def school_university_save_results_to_s3(**context):
    query_results = context['ti'].xcom_pull(task_ids='school_university_run_select_query')
    column_names = ["seq", "schoolName", "campusName", "schoolGubun", "schoolType", "schoolEmail", "estType", "region", "link", "total_count", "is_show"]
    df = pd.DataFrame(query_results, columns=column_names)
    school_university_save_to_s3_with_hook(df, 'onuii-data-pipeline', 'school_university', user_filename)

def school_university_insert_postgres_data(**context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    records = context['ti'].xcom_pull(task_ids='school_university_run_select_query')
    insert_query = warehouse_query2.school_university_insert_query

    pg_hook = PostgresHook(postgres_conn_id='postgres_dev_conn')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    for record in records:
        pg_cursor.execute(insert_query, (
            record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9], record[10]
        ))

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()





default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'data-warehouse-test-postgresql-school_university_2.0',
    default_args=default_args,
    description='Run query and load result to S3',
    schedule='40 17 * * *',
    tags=["2.0"]
)



#user
school_university_run_query = SQLExecuteQueryOperator(
    task_id='school_university_run_select_query',
    sql=warehouse_query2.school_university_select_query,
    conn_id='legacy_staging_conn',
    do_xcom_push=True,
    dag=dag,
)

school_university_delete_row = SQLExecuteQueryOperator(
    task_id="school_university_delete_row",
    conn_id='postgres_dev_conn',
    sql=warehouse_query2.school_university_delete_query
)

school_university_insert_data = PythonOperator(
    task_id='school_university_insert_data',
    python_callable=school_university_insert_postgres_data,
    provide_context=True,
)

school_university_save_to_s3_task = PythonOperator(
    task_id='user_save_to_s3',
    python_callable=school_university_save_results_to_s3,
    provide_context=True,
)






# dbt_run = BashOperator(
#     task_id='dbt_run',
#     bash_command='dbt run --profiles-dir /opt/airflow/dbt_project/.dbt --project-dir /opt/airflow/dbt_project --models /opt/airflow/dbt_project/models/pg_active_lecture/active_lecture.sql',
# )

school_university_run_query >> school_university_delete_row >> school_university_insert_data >> school_university_save_to_s3_task 


