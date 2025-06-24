import sys
import os

# 현재 파일이 있는 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO


date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

trino_database = 'mysql'

trino_schema = 'onuei'

pg_schema = 'raw_data'

table_name = 'tutoring_state_history'

date_column = 'created_at'

column_list = ["lectur_vt_No","tutoring_state","created_at"]
columns_str = ", ".join(f'"{col}"' for col in column_list)

pk = 'lecture_vt_no'

filename = table_name+date + '.csv'




    

# S3 버킷 및 디렉토리 지정
def save_to_s3_with_hook(data, bucket_name, version, folder_name, file_name):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    hook = S3Hook(aws_conn_id='conn_S3')
    hook.load_string(csv_buffer.getvalue(), key=f"{version}/{folder_name}/{file_name}", bucket_name=bucket_name, replace=True)


# incremental_extract 결과 받아와서 S3에 저장
def save_results_to_s3(**context):
    query_results = context['ti'].xcom_pull(task_ids='incremental_extract_and_load')
    column_names = column_list
    df = pd.DataFrame(query_results, columns=column_names)
    save_to_s3_with_hook(df, 'onuii-data-pipeline', 'staging',table_name, filename)


def incremental_extract():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.trino.hooks.trino import TrinoHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_2.0')  
    trino_hook = TrinoHook(trino_conn_id='trino_conn')   

    pg_engine = pg_hook.get_sqlalchemy_engine()
    trino_engine = trino_hook.get_sqlalchemy_engine()

    # 1. 이전 데이터 불러오기 (최근 상태만)
    table_check_query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = '{pg_schema}' 
        AND table_name = '{table_name}'
    ) AS table_exists;
    """
    table_exists_result = pd.read_sql(table_check_query, pg_engine)
    table_exists = table_exists_result['table_exists'].iloc[0]

    if table_exists:
        max_created_query = f'SELECT MAX(created_at) AS max_created_at FROM {pg_schema}."{table_name}"'
        max_created_result = pd.read_sql(max_created_query, pg_engine)
        max_created_at = max_created_result['max_created_at'].iloc[0]
        if pd.isnull(max_created_at):
            max_created_at = '2019-01-01 00:00:00'
    else:
        max_created_at = '2019-01-01 00:00:00'

    # 2. 최신 상태만 추출
    today_data_query = f'''
        SELECT lecture_vt_No, tutoring_state
        FROM {trino_database}.{trino_schema}.lecture_video_tutoring
        WHERE update_datetime > TIMESTAMP '{max_created_at}'
    '''
    df_today = pd.read_sql(today_data_query, trino_engine)

    # 3. PostgreSQL에서 lecture_vt_No 기준 최신값 조회
    if table_exists:
        latest_query = f"""
            SELECT DISTINCT ON ("{pk}") "{pk}", tutoring_state
            FROM {pg_schema}."{table_name}"
            ORDER BY "{pk}", created_at DESC
        """
        df_before = pd.read_sql(latest_query, pg_engine)
    else:
        df_before = pd.DataFrame(columns=[pk, "tutoring_state"])

    # 4. 변경 비교
    df_today['created_at'] = datetime.now()
    df_merged = df_today.merge(df_before, on=pk, how='left', suffixes=('', '_old'))
    df_changed = df_merged[
        (df_merged['tutoring_state'] != df_merged['tutoring_state_old']) |
        (df_merged['tutoring_state_old'].isnull())
    ][['lecture_vt_No', 'tutoring_state', 'created_at']]

    # 5. 변경된 것만 append
    df_changed.to_sql(
        name=table_name,
        con=pg_engine,
        schema=pg_schema,
        if_exists='append',
        index=False
    )





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
    description='Run query and load result to S3',
    schedule='10 17 * * *',
    tags=['2.0','tutoring','raw'],
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


