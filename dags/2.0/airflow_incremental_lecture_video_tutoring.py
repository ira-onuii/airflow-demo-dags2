import sys
import os

# 현재 파일이 있는 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import warehouse_query
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

filename = 'pg_lecture_video_tutoring_'+date + '.csv'



# S3 버킷 지정
def save_to_s3_with_hook(data, bucket_name, folder_name, file_name, **kwargs):
    csv_buffer = StringIO()
    s3_key = f"{folder_name}/{file_name}" 
    data.to_csv(csv_buffer, index=False)
    hook = S3Hook(aws_conn_id='conn_S3')
    hook.load_string(csv_buffer.getvalue(), key=s3_key, bucket_name=bucket_name, replace=True)
    kwargs['ti'].xcom_push(key='bucket_name', value=bucket_name)
    kwargs['ti'].xcom_push(key='s3_key', value=s3_key)




# 증분 추출 with row_number()
def incremental_extract(**kwargs):
    #from sqlalchemy import get_sqlalchemy_engine
    from dotenv import load_dotenv
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.mysql.hooks.mysql import MySqlHook

    load_dotenv()
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_dev_conn')  
    mysql_hook = MySqlHook(mysql_conn_id='legacy_staging_conn')   

    # SQLAlchemy Engine 생성
    pg_engine = pg_hook.get_sqlalchemy_engine()
    mysql_engine = mysql_hook.get_sqlalchemy_engine()

    before_data = 'select * from raw_data.lecture_video_tutoring'
    today_data = warehouse_query.lvt_select_query

    df_before = pd.read_sql(before_data, pg_engine)
    df_today = pd.read_sql(today_data, mysql_engine)
    df_union_all = pd.concat([df_before, df_today], ignore_index=True)

    #df_union_all['update_datetime'] = pd.to_datetime(df_union_all['update_datetime'], errors='coerce')


    df_union_all['row_number'] = df_union_all.sort_values(by = ['update_datetime'], ascending = False).groupby(['lecture_vt_no']).cumcount()+1
    df_union_all = df_union_all[df_union_all['row_number'] == 1]

    save_to_s3_with_hook(df_union_all, 'onuii-data-pipeline', 'lecture_video_tutoring', filename, **kwargs)

    
def read_s3_and_insert_db(**kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import csv
    # XCom에서 S3 정보 가져오기
    ti = kwargs['ti']
    bucket_name = ti.xcom_pull(task_ids='incremental_extract_and_save_to_s3', key='bucket_name')
    s3_key = ti.xcom_pull(task_ids='incremental_extract_and_save_to_s3', key='s3_key')

    # S3 파일 읽기
    s3_hook = S3Hook(aws_conn_id='conn_S3')
    file_obj = s3_hook.get_key(bucket_name=bucket_name, key=s3_key)
    csv_data = file_obj.get()['Body'].read().decode('utf-8').splitlines()    
    pg_hook = PostgresHook(postgres_conn_id='postgres_dev_conn')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    csv_reader = csv.reader(csv_data)
    header = next(csv_reader)  # 헤더 읽기
    insert_query = f"INSERT INTO test ({', '.join(header)}) VALUES ({', '.join(['%s'] * len(header))})"

    # 행 단위로 데이터 삽입
    for row in csv_reader:
        pg_cursor.execute(insert_query, row)

    # 커밋 및 연결 종료
    pg_conn.commit()
    pg_cursor.close()






default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'data-warehouse-test-postgresql-lvt-incremental',
    default_args=default_args,
    description='Run query and load result to S3',
    schedule='10 17 * * *',
)

#lvt
incremental_extract_and_save_to_s3 = PythonOperator(
    task_id='incremental_extract_and_save_to_s3',
    python_callable=incremental_extract,
    provide_context=True,
    dag=dag
)

delete_row = SQLExecuteQueryOperator(
    task_id="delete_row",
    conn_id='postgres_dev_conn',
    sql=warehouse_query.lvt_delete_query,
    dag=dag
)

insert_data = PythonOperator(
    task_id='insert_lvt_data',
    python_callable=read_s3_and_insert_db,
    provide_context=True,
    dag=dag
)

# dbt_run = BashOperator(
#     task_id='dbt_run',
#     bash_command='dbt run --profiles-dir /opt/airflow/dbt_project/.dbt --project-dir /opt/airflow/dbt_project --models /opt/airflow/dbt_project/models/pg_active_lecture/active_lecture.sql',
# )

incremental_extract_and_save_to_s3 >> delete_row >> insert_data


