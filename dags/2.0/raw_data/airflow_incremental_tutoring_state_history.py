import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO


# 경로 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 설정 값
date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))
trino_database = 'mysql'
trino_schema = 'onuei'
pg_schema = 'raw_data'
table_name = 'tutoring_state_history'
date_column = 'created_at'
column_list = ["lecture_vt_no", "tutoring_state", "created_at"]
columns_str = ", ".join(f'"{col}"' for col in column_list)
pk = 'lecture_vt_no'
filename = table_name + date + '.csv'


# S3 업로드 함수
def save_to_s3_with_hook(data, bucket_name, version, folder_name, file_name):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    hook = S3Hook(aws_conn_id='conn_S3')
    hook.load_string(csv_buffer.getvalue(), key=f"{version}/{folder_name}/{file_name}", bucket_name=bucket_name, replace=True)


# S3 저장 태스크
def save_results_to_s3(**context):
    query_results = context['ti'].xcom_pull(task_ids='incremental_extract_and_load')
    df = pd.DataFrame(query_results, columns=column_list)
    save_to_s3_with_hook(df, 'onuii-data-pipeline', 'staging', table_name, filename)


# 증분 추출 및 변경값 저장
def incremental_extract():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.trino.hooks.trino import TrinoHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_2.0')  
    trino_hook = TrinoHook(trino_conn_id='trino_conn')   

    pg_engine = pg_hook.get_sqlalchemy_engine()
    trino_engine = trino_hook.get_sqlalchemy_engine()

    # 테이블 존재 여부 확인
    table_check_query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = '{pg_schema}' 
            AND table_name = '{table_name}'
        ) AS table_exists;
    """
    table_exists_result = pd.read_sql(table_check_query, pg_engine)
    table_exists = table_exists_result['table_exists'].iloc[0]

    # PostgreSQL에서 가장 최근 created_at 구함
    if table_exists:
        max_created_query = f'SELECT MAX(created_at) AS max_created_at FROM {pg_schema}."{table_name}"'
        max_created_result = pd.read_sql(max_created_query, pg_engine)
        max_created_at = max_created_result['max_created_at'].iloc[0]
        if pd.isnull(max_created_at):
            max_created_at = '2019-01-01 00:00:00'
    else:
        max_created_at = '2019-01-01 00:00:00'

    # Trino에서 증분 데이터 가져오기
    today_data_query = f'''
        SELECT 
            lecture_vt_No AS lecture_vt_no,
            tutoring_state,
            cast(now()+interval '9' hour as varchar) AS created_at
        FROM {trino_database}.{trino_schema}.lecture_video_tutoring
        WHERE update_datetime > cast('{max_created_at}' as timestamp)
    '''
    df_today = pd.read_sql(today_data_query, trino_engine)

    if df_today.empty:
        print("✅ Trino에서 새로운 변경 데이터 없음.")
        return []

    #df_today['created_at'] = datetime.now()

    # PostgreSQL의 최신 상태 조회
    if table_exists:
        latest_query = f"""
            SELECT DISTINCT ON ({pk}) {pk}, tutoring_state
            FROM {pg_schema}."{table_name}"
            ORDER BY {pk}, created_at DESC
        """
        df_before = pd.read_sql(latest_query, pg_engine)
    else:
        df_before = pd.DataFrame(columns=[pk, 'tutoring_state'])

    # 비교하여 변경된 행만 필터링
    df_merged = df_today.merge(df_before, on=pk, how='left', suffixes=('', '_old'))
    df_changed = df_merged[
        (df_merged['tutoring_state'] != df_merged['tutoring_state_old']) |
        (df_merged['tutoring_state_old'].isnull())
    ][['lecture_vt_no', 'tutoring_state', 'created_at']]

    if df_changed.empty:
        print("✅ 상태가 변경된 행 없음.")
        return []

    # 변경된 행만 PostgreSQL에 append
    df_changed.to_sql(
        name=table_name,
        con=pg_engine,
        schema=pg_schema,
        if_exists='append',
        index=False
    )

    return df_changed.values.tolist()


# Airflow DAG 정의
default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    f'data-warehouse-test-postgresql-{table_name}-incremental_2.0',
    default_args=default_args,
    description='Incremental extract and append history to PostgreSQL and S3',
    schedule='10 17 * * *',
    tags=['2.0', 'tutoring', 'raw'],
    catchup=False
)

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

incremental_extract_and_load >> load_S3
