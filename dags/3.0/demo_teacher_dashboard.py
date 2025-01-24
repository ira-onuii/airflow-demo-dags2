import sys
import os
import random

# 현재 파일이 있는 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO


date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

pg_schema = 'dashboard_demo'


start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 1, 31)







def join_lst():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_3.0')  
   
    pg_engine = pg_hook.get_sqlalchemy_engine()

    random_id2 = []

    for i in range(1000):
        random_id2.append([random.randrange(2000000,2001000), (start_date + timedelta(days=random.randint(0, (end_date - start_date).days)))])

    data = pd.DataFrame(random_id2, columns=['teacher_id','created_at'])

    data['seq'] = data.sort_values(by = ['created_at'], ascending = False).groupby(['teacher_id']).cumcount()+1

    join_list = data[['teacher_id','created_at','seq']]
    
    join_list.to_sql(
        name= 'join_teacher_demo',  # 삽입할 테이블 이름
        con=pg_engine,  # PostgreSQL 연결 엔진
        schema=pg_schema,
        if_exists='replace',  # 테이블이 있으면 삭제 후 재생성
        index=False  # DataFrame 인덱스는 삽입하지 않음
    )

    
    

def under_review_lst():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_3.0')  
   
    pg_engine = pg_hook.get_sqlalchemy_engine()

    random_id3 = []

    for i in range(1000):
        type = ["submitted_application","passed_application","submitted_voice","not_submitted_voice","passed_voice","not_signed","signed","completed_education","not_completed_education"]
        random_id3.append([random.randrange(2000000,2001000),random.choice(type), (start_date + timedelta(days=random.randint(0, (end_date - start_date).days)))])
        
    data = pd.DataFrame(random_id3, columns=['teacher_id','tag','created_at'])

    data['seq'] = data.sort_values(by = ['created_at'], ascending = False).groupby(['teacher_id','tag']).cumcount()+1

    under_review_list = data[['teacher_id','tag','created_at','seq']]
    
    under_review_list.to_sql(
        name= 'under_review_teacher_demo',  # 삽입할 테이블 이름
        con=pg_engine,  # PostgreSQL 연결 엔진
        schema=pg_schema,
        if_exists='replace',  # 테이블이 있으면 삭제 후 재생성
        index=False  # DataFrame 인덱스는 삽입하지 않음
    )


def drop_lst():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_3.0')  
   
    pg_engine = pg_hook.get_sqlalchemy_engine()

    random_id3 = []

    for i in range(1000):
        type = ["drop_at_application","drop_at_voice"]
        random_id3.append([random.randrange(2000000,2001000),random.choice(type), (start_date + timedelta(days=random.randint(0, (end_date - start_date).days)))])
        
    data = pd.DataFrame(random_id3, columns=['teacher_id','tag','created_at'])

    data['seq'] = data.sort_values(by = ['created_at'], ascending = False).groupby(['teacher_id','tag']).cumcount()+1

    under_review_list = data[['teacher_id','tag','created_at','seq']]
    
    under_review_list.to_sql(
        name= 'drop_teacher_demo',  # 삽입할 테이블 이름
        con=pg_engine,  # PostgreSQL 연결 엔진
        schema=pg_schema,
        if_exists='replace',  # 테이블이 있으면 삭제 후 재생성
        index=False  # DataFrame 인덱스는 삽입하지 않음
    )


def active_lst():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_3.0')  
   
    pg_engine = pg_hook.get_sqlalchemy_engine()

    random_id3 = []

    for i in range(1000):
        type = ["active","dormant"]
        random_id3.append([random.randrange(2000000,2001000),random.choice(type), (start_date + timedelta(days=random.randint(0, (end_date - start_date).days)))])
        
    data = pd.DataFrame(random_id3, columns=['teacher_id','tag','created_at'])

    data['seq'] = data.sort_values(by = ['created_at'], ascending = False).groupby(['teacher_id','tag']).cumcount()+1

    under_review_list = data[['teacher_id','tag','created_at','seq']]
    
    under_review_list.to_sql(
        name= 'active_teacher_demo',  # 삽입할 테이블 이름
        con=pg_engine,  # PostgreSQL 연결 엔진
        schema=pg_schema,
        if_exists='replace',  # 테이블이 있으면 삭제 후 재생성
        index=False  # DataFrame 인덱스는 삽입하지 않음
    )


def done_lst():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_3.0')  
   
    pg_engine = pg_hook.get_sqlalchemy_engine()

    random_id3 = []

    for i in range(1000):
        type = ["normal","penalty"]
        random_id3.append([random.randrange(2000000,2001000),random.choice(type), (start_date + timedelta(days=random.randint(0, (end_date - start_date).days)))])
        
    data = pd.DataFrame(random_id3, columns=['teacher_id','tag','created_at'])

    data['seq'] = data.sort_values(by = ['created_at'], ascending = False).groupby(['teacher_id','tag']).cumcount()+1

    under_review_list = data[['teacher_id','tag','created_at','seq']]
    
    under_review_list.to_sql(
        name= 'done_teacher_demo',  # 삽입할 테이블 이름
        con=pg_engine,  # PostgreSQL 연결 엔진
        schema=pg_schema,
        if_exists='replace',  # 테이블이 있으면 삭제 후 재생성
        index=False  # DataFrame 인덱스는 삽입하지 않음
    )


def dm_lst(indicator_table,column_name1,column_name2):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_3.0')  
   
    pg_engine = pg_hook.get_sqlalchemy_engine()

    random_id2 = []

    for i in range(1000):
        random_id2.append([random.randrange(1000000,2000000), (start_date + timedelta(days=random.randint(0, (end_date - start_date).days))),round(random.uniform(0, 24),3)])

    data = pd.DataFrame(random_id2, columns=[column_name1,'created_at',column_name2])
    
    data.to_sql(
        name= indicator_table,  # 삽입할 테이블 이름
        con=pg_engine,  # PostgreSQL 연결 엔진
        schema=pg_schema,
        if_exists='replace',  # 테이블이 있으면 삭제 후 재생성
        index=False  # DataFrame 인덱스는 삽입하지 않음
    )
    
    



default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    f'data-warehouse-test-postgresql-teacher_dashboard_demo-incremental_3.0',
    default_args=default_args,
    description='Run query and load result to S3',
    schedule='10 17 * * *',
    tags=['demo','dashboard']
)


join_teacher = PythonOperator(
    task_id='join_teacher',
    python_callable=join_lst,
    #op_kwargs={"indicator_table": "add_subject_payment","column_name1": "payment_id","column_name2": "amount"},
    provide_context=True,
    dag=dag
)

under_review_teacher = PythonOperator(
    task_id='under_review_teacher',
    python_callable=under_review_lst,
    #op_kwargs={"indicator_table": "add_subject_payment","column_name1": "payment_id","column_name2": "amount"},
    provide_context=True,
    dag=dag
)

drop_teacher = PythonOperator(
    task_id='drop_teacher',
    python_callable=drop_lst,
    #op_kwargs={"indicator_table": "add_subject_payment","column_name1": "payment_id","column_name2": "amount"},
    provide_context=True,
    dag=dag
)

active_teacher = PythonOperator(
    task_id='active_teacher',
    python_callable=active_lst,
    #op_kwargs={"indicator_table": "add_subject_payment","column_name1": "payment_id","column_name2": "amount"},
    provide_context=True,
    dag=dag
)

done_teacher = PythonOperator(
    task_id='done_teacher',
    python_callable=done_lst,
    #op_kwargs={"indicator_table": "add_subject_payment","column_name1": "payment_id","column_name2": "amount"},
    provide_context=True,
    dag=dag
)


join_teacher >> under_review_teacher >> drop_teacher >> active_teacher >> done_teacher
