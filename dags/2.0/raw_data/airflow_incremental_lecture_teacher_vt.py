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

table_name = 'lecture_teacher_vt'

date_column = 'update_at'

column_list = ["lecture_teacher_vt_no","teacher_user_no","lecture_vt_no","last_schedule_no","teacher_vt_status","academic_departments","teacher_academic_major","division_of_matching_standard","active_done_month","total_done_month","reactive_at","create_at","update_at","lecture_subject_id"]
columns_str = ", ".join(f'"{col}"' for col in column_list)

pk = 'lecture_teacher_vt_no'

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


# 증분 추출 with row_number()
def incremental_extract():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.trino.hooks.trino import TrinoHook


    # postgresql 연결
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_2.0')  
    # trino 연결
    trino_hook = TrinoHook(trino_conn_id='trino_conn')   

    # SQLAlchemy Engine 생성
    pg_engine = pg_hook.get_sqlalchemy_engine()
    trino_engine = trino_hook.get_sqlalchemy_engine()


    # 1. 테이블 존재 여부 확인
    table_check_query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = '{pg_schema}' 
        AND table_name = '{table_name}'
    ) AS table_exists;
    """
    
    table_exists_result = pd.read_sql(table_check_query, pg_engine)
    table_exists = table_exists_result['table_exists'].iloc[0]

    # 2. 분기 처리
    if table_exists:
        print('###True###')
        before_data_query = f'SELECT {columns_str} FROM {pg_schema}."{table_name}"'
        print(before_data_query)
        max_updated_query = f'SELECT MAX({date_column}) AS max_updatedat FROM {pg_schema}."{table_name}"'
        print(max_updated_query)
        
        max_updated_result = pd.read_sql(max_updated_query, pg_engine)
        max_updatedat = max_updated_result['max_updatedat'].iloc[0]
        if max_updatedat is None:
            max_updatedat = '2019-01-01 00:00:00'
        
        df_before = pd.read_sql(before_data_query, pg_engine)

    else:
        print('###False###')
        max_updatedat = '2019-01-01 00:00:00'
        df_before = pd.DataFrame(columns=column_list) 

    print(f"기준 시각: {max_updatedat}")

    # 최근 실행시점 이후 update된 데이터 추출 쿼리
    today_data_query = f'''
       select 
        {columns_str}
        from "{trino_database}"."{trino_schema}".{table_name}
        where {date_column} > cast('{max_updatedat}' as timestamp)
    '''
    print(today_data_query)

  
 
    df_today = pd.read_sql(today_data_query, trino_engine)
    print(df_today)

    # 4. 병합 및 최신 row 추출
    df_union_all = pd.concat([df_before, df_today], ignore_index=True)

    df_union_all['row_number'] = df_union_all.sort_values(
        by=date_column, ascending=False
    ).groupby([pk]).cumcount() + 1

    df_incremental = df_union_all[df_union_all['row_number'] == 1]

    # 5. 저장
    df_incremental.to_sql(
        name=table_name,
        con=pg_engine,
        schema=pg_schema,
        if_exists='replace',
        index=False
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


