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
from pendulum import timezone

KST = timezone("Asia/Seoul")


date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))


pg_schema = 'raw_data'

table_name = 'lecture_vt_cycles_250401-250831_v2'

#date_column = 'updated_at'

column_list = ["lecture_cycle_no","lecture_vt_no","page_call_room_id","review","cycle_review","req_datetime","update_datetime"]
columns_str = ", ".join(f'"{col}"' for col in column_list)

pk = 'lecture_cycle_no'

tags = ['2.0','raw','lecture']

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



# 증분 추출 
def incremental_extract():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.trino.hooks.trino import TrinoHook
    # from warehouse_query import group_lvt_query

    # postgresql 연결
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_2.0')  
    # trino 연결
    trino_hook = TrinoHook(trino_conn_id='trino_conn')   

    # SQLAlchemy Engine 생성
    pg_engine = pg_hook.get_sqlalchemy_engine()
    trino_engine = trino_hook.get_sqlalchemy_engine()


    

    # 최근 실행시점 이후 update된 데이터 추출 쿼리

    today_data_query = '''
select lvc.lecture_cycle_no, lvc.lecture_vt_no, lvc.page_call_room_id, lvc.review, lvc.cycle_review, lvc.req_datetime, lvc.update_datetime 
	from mysql.onuei.lecture_vt_cycles lvc
	where lvc.req_datetime between date '2025-04-01' and date '2025-08-31'
'''

 
    df_today = pd.read_sql(today_data_query, trino_engine)
    print(df_today)


    # 5. 저장
    df_today.to_sql(
        name=table_name,
        con=pg_engine,
        schema=pg_schema,
        if_exists='replace',
        index=False
    )

    return df_today

    





default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2024, 1, 1, tzinfo=KST),
}

dag = DAG(
    f'data-warehouse-test-postgresql-{table_name}-incremental_2.0',
    default_args=default_args,
    description='Run query and load result to S3',
    schedule='0 8,13,16,0 * * *',
    tags=tags,
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


