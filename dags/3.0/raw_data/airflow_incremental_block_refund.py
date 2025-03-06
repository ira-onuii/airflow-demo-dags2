import sys
import os

# 현재 파일이 있는 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import warehouse_query3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO


date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

trino_database = 'payment_live_mysql'

trino_schema = 'payment'

pg_schema = 'raw_data'

table_name = 'block_refund'

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
    column_names = ['id','refundedat','requestedat','state','memo','amount','paymentitemid','blockid','detailid','refundbankid','refundmethod']
    df = pd.DataFrame(query_results, columns=column_names)
    save_to_s3_with_hook(df, 'onuii-data-pipeline-3.0', 'live',table_name, filename)


# 증분 추출 with row_number()
def incremental_extract():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    from airflow.providers.trino.hooks.trino import TrinoHook

    # postgresql 연결
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_3.0')  
    # mysql 연결
    trino_hook = TrinoHook(trino_conn_id='trino_conn')   

    # SQLAlchemy Engine 생성
    pg_engine = pg_hook.get_sqlalchemy_engine()
    trino_engine = trino_hook.get_sqlalchemy_engine()

    # 기존 data warehouse에 있던 max id
    before_data = f'select max(id) as max_id from {pg_schema}."{trino_schema}.{table_name}"'
    before_data_result =  pd.read_sql(before_data, pg_engine)
    max_id = before_data_result['max_id'].iloc[0]
    print(max_id)

    # 최근 실행시점 이후 update된 데이터 추출 쿼리
    today_data = f'''
    select 
        "id","refundedat","requestedat","state","memo","amount","paymentitemid","blockid","detailid","refundbankid","refundmethod"
        from {trino_database}.{trino_schema}.{table_name}
        where id > ({max_id})
    '''

    # 오늘 쿼리 실행
    df_today = pd.read_sql(today_data, trino_engine)
    
    # 컬럼 순서 정렬
    df_incremental = df_today[['id','refundedat','requestedat','state','memo','amount','paymentitemid','blockid','detailid','refundbankid','refundmethod']]


    # 정제된 데이터 data_warehouse 테이블에 삽입
    df_incremental.to_sql(
        name=trino_schema+'.'+table_name,  # 삽입할 테이블 이름
        schema=pg_schema,
        con=pg_engine,  # PostgreSQL 연결 엔진
        if_exists='append',  # 테이블이 있으면 삭제 후 재생성
        index=False  # DataFrame 인덱스는 삽입하지 않음
    )

    return df_incremental

    





default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    f'data-warehouse-test-postgresql-{table_name}-incremental_3.0',
    default_args=default_args,
    description='Run query and load result to S3',
    schedule='10 17 * * *',
    tags=['3.0','payment','raw']
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


