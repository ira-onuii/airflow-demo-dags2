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

trino_database = '3.0_item_mongodb_live'

trino_schema = 'item'

pg_schema = 'raw_data'

table_name = 'fixed_package'

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
    column_names = ['createdat','updatedat','id','name','packageid','type','price','taxtype','grade','subject','count_per_week','minutes','months']
    df = pd.DataFrame(query_results, columns=column_names)
    save_to_s3_with_hook(df, 'onuii-data-pipeline-3.0', 'staging',table_name, filename)


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

    

    # # 기존 data warehouse에 있던 데이터 추출 쿼리
    before_data = f'select * from {pg_schema}."{trino_schema}.{table_name}"'

     # 기존 data warehouse에 있던 마지막 updatedat
    max_updated_data = f'select max(updatedat) as max_updatedat from {pg_schema}."{trino_schema}.{table_name}"'
    max_updated_data_result =  pd.read_sql(max_updated_data, pg_engine)
    max_updatedat = max_updated_data_result['max_updatedat'].iloc[0]
    if max_updatedat is None:
        max_updatedat = '2019-01-01 00:00:00'  # 기본값
    else:
        max_updatedat
    print(max_updatedat)

    # 최근 실행시점 이후 update된 데이터 추출 쿼리
    today_data = f'''
        select createdat, updatedat, id, name, packageid, "type", price, taxtype
            , split(name, '|')[1] AS grade
            , split(name, '|')[2] AS subject
            , substring(split(name, '|')[3],4,1) AS count_per_week
            , substring(split(name, '|')[4],2,2) AS minutes
            , substring(split(name, '|')[5],2,1) AS months
            from "3.0_item_mongodb_live".item.fixedpackage fp
            where fp.blocks is not null
            and "type" = 'LECTURE'
            and name like '%|%'
            and updatedat > cast('{max_updatedat}' as timestamp)
    '''
    #and updatedat > cast('{max_updatedat}' as timestamp)

    # 쿼리 실행 및 union all로 병합
    df_before = pd.read_sql(before_data, pg_engine)
    print(f"before data Number of rows: {len(df_before)}")
    df_today = pd.read_sql(today_data, trino_engine)
    print(f"today data Number of rows: {len(df_today)}")
    df_union_all = pd.concat([df_before, df_today], ignore_index=True)
    print(f"union all data Number of rows: {len(df_union_all)}")


    # # date_type 변환
    # df_union_all['update_datetime'] = pd.to_datetime(df_union_all['update_datetime'], errors='coerce')

    # PK 값 별 최근 행이 1이 오도록 row_number 설정
    df_union_all['row_number'] = df_union_all.sort_values(by = ['updatedat'], ascending = False).groupby(['id']).cumcount()+1


    # PK 값 별 최근 행만 추출
    df_incremental = df_union_all[df_union_all['row_number'] == 1]
    print(f"final data Number of rows: {len(df_incremental)}")
    print(df_incremental)
    
    # row_number 컬럼 제거 및 컬럼 순서 정렬
    df_incremental = df_incremental[['createdat','updatedat','id','name','packageid','type','price','taxtype','grade','subject','count_per_week','minutes','months']]

    # # 특정 컬럼만 NaN 처리 후 int로 변환
    # df_incremental[['payment_item', 'next_payment_item', 'current_schedule_no']] = (
    #     df_incremental[['payment_item', 'next_payment_item', 'current_schedule_no']]
    #     .fillna(0)  # NaN은 0으로 대체
    #     .astype('int64')  # 정수형으로 변환
    # )

    # 정제된 데이터 data_warehouse 테이블에 삽입
    df_incremental.to_sql(
        name= trino_schema+'.'+table_name,  # 삽입할 테이블 이름
        con=pg_engine,  # PostgreSQL 연결 엔진
        schema=pg_schema,
        if_exists='replace',  # 테이블이 있으면 삭제 후 재생성
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
    tags=['3.0','item']
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


incremental_extract_and_load >> load_S3


