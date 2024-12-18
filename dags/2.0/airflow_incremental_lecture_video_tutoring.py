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






# def save_to_s3_with_hook(data, bucket_name, folder_name, file_name, **kwargs):
#     csv_buffer = StringIO()
#     s3_key = f"{folder_name}/{file_name}" 
#     data.to_csv(csv_buffer, index=False)
#     hook = S3Hook(aws_conn_id='conn_S3')
#     hook.load_string(csv_buffer.getvalue(), key=s3_key, bucket_name=bucket_name, replace=True)
#     kwargs['ti'].xcom_push(key='bucket_name', value=bucket_name)
#     kwargs['ti'].xcom_push(key='s3_key', value=s3_key)
    

# S3 버킷 및 디렉토리 지정
def save_to_s3_with_hook(data, bucket_name, folder_name, file_name):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    hook = S3Hook(aws_conn_id='conn_S3')
    hook.load_string(csv_buffer.getvalue(), key=f"{folder_name}/{file_name}", bucket_name=bucket_name, replace=True)


# incremental_extract 결과 받아와서 S3에 저장
def save_results_to_s3(**context):
    query_results = context['ti'].xcom_pull(task_ids='incremental_extract_and_load')
    column_names = ['lecture_vt_no','student_user_no','lecture_subject_id','student_type','tutoring_state','payment_item','next_payment_item','current_schedule_no','stage_max_cycle_count','stage_free_cycle_count','stage_pre_offer_cycle_count','stage_offer_cycle_count','create_datetime','update_datetime','last_done_datetime','application_datetime','memo','total_subject_done_month','reactive_datetime']
    df = pd.DataFrame(query_results, columns=column_names)
    save_to_s3_with_hook(df, 'onuii-data-pipeline', 'lecture_video_tutoring', filename)


# 증분 추출 with row_number()
def incremental_extract(**kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.mysql.hooks.mysql import MySqlHook

    # postgresql 연결
    pg_hook = PostgresHook(postgres_conn_id='postgres_dev_conn')  
    # mysql 연결
    mysql_hook = MySqlHook(mysql_conn_id='legacy_staging_conn')   

    # SQLAlchemy Engine 생성
    pg_engine = pg_hook.get_sqlalchemy_engine()
    mysql_engine = mysql_hook.get_sqlalchemy_engine()

    # 기존 data warehouse에 있던 데이터 추출 쿼리
    before_data = 'select * from raw_data.lecture_video_tutoring'

    # 최근 실행시점 이후 update된 데이터 추출 쿼리
    today_data = warehouse_query.lvt_select_query

    # 쿼리 실행 및 union all로 병합
    df_before = pd.read_sql(before_data, pg_engine)
    df_today = pd.read_sql(today_data, mysql_engine)
    df_union_all = pd.concat([df_before, df_today], ignore_index=True)

    # date_type 변환
    df_union_all['update_datetime'] = pd.to_datetime(df_union_all['update_datetime'], errors='coerce')

    # PK 값 별 최근 행이 1이 오도록 row_number 설정
    df_union_all['row_number'] = df_union_all.sort_values(by = ['update_datetime'], ascending = False).groupby(['lecture_vt_no']).cumcount()+1
    
    # PK 값 별 최근 행만 추출
    df_incremental = df_union_all[df_union_all['row_number'] == 1]
    
    # row_number 컬럼 제거 및 컬럼 순서 정렬
    df_incremental = df_incremental[['lecture_vt_no','student_user_no','lecture_subject_id','student_type','tutoring_state','payment_item','next_payment_item','current_schedule_no','stage_max_cycle_count','stage_free_cycle_count','stage_pre_offer_cycle_count','stage_offer_cycle_count','create_datetime','update_datetime','last_done_datetime','application_datetime','memo','total_subject_done_month','reactive_datetime']]

    # 특정 컬럼만 NaN 처리 후 int로 변환
    df_incremental[['payment_item', 'next_payment_item', 'current_schedule_no']] = (
        df_incremental[['payment_item', 'next_payment_item', 'current_schedule_no']]
        .fillna(0)  # NaN은 0으로 대체
        .astype('int64')  # 정수형으로 변환
    )

    # 정제된 데이터 data_warehouse 테이블에 삽입
    df_incremental.to_sql(
        name='lecture_video_tutoring',  # 삽입할 테이블 이름
        con=pg_engine,  # PostgreSQL 연결 엔진
        if_exists='replace',  # 테이블이 있으면 삭제 후 재생성
        index=False  # DataFrame 인덱스는 삽입하지 않음
    )

    return df_incremental

    

    
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
    insert_query = f"INSERT INTO raw_data.lecture_video_tutoring ({', '.join(header)}) VALUES ({', '.join(['%s'] * len(header))})"

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
incremental_extract_and_load = PythonOperator(
    task_id='incremental_extract_and_load',
    python_callable=incremental_extract,
    provide_context=True,
    dag=dag
)

load_S3 = PythonOperator(
    task_id='load_S3',
    python_callable=save_results_to_s3,
    provide_context=True,
    dag=dag
)

# delete_row = SQLExecuteQueryOperator(
#     task_id="delete_row",
#     conn_id='postgres_dev_conn',
#     sql=warehouse_query.lvt_delete_query,
#     dag=dag
# )

# insert_data = PythonOperator(
#     task_id='insert_lvt_data',
#     python_callable=read_s3_and_insert_db,
#     provide_context=True,
#     dag=dag
# )

# dbt_run = BashOperator(
#     task_id='dbt_run',
#     bash_command='dbt run --profiles-dir /opt/airflow/dbt_project/.dbt --project-dir /opt/airflow/dbt_project --models /opt/airflow/dbt_project/models/pg_active_lecture/active_lecture.sql',
# )

incremental_extract_and_load >> load_S3


