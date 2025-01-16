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





def one_lst(indicator_table):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_3.0')  
   
    pg_engine = pg_hook.get_sqlalchemy_engine()

    # 빈 리스트 생성
    random_id1 = []

    # 값 추가
    for i in range(1000):
        random_id1.append(random.randrange(1000000, 2000000))

    # DataFrame 생성
    data = pd.DataFrame(random_id1, columns=['id'])
    
    data.to_sql(
        name= indicator_table,  # 삽입할 테이블 이름
        con=pg_engine,  # PostgreSQL 연결 엔진
        schema=pg_schema,
        if_exists='append',  # 테이블이 있으면 삭제 후 재생성
        index=False  # DataFrame 인덱스는 삽입하지 않음
    )


def two_lst(indicator_table):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_3.0')  
   
    pg_engine = pg_hook.get_sqlalchemy_engine()

    random_id2 = []

    for i in range(1000):
        random_id2.append([random.randrange(1000000,2000000),random.randrange(300000,2000000)])

    data = pd.DataFrame(random_id2, columns=['id','amount'])
    
    data.to_sql(
        name= indicator_table,  # 삽입할 테이블 이름
        con=pg_engine,  # PostgreSQL 연결 엔진
        schema=pg_schema,
        if_exists='append',  # 테이블이 있으면 삭제 후 재생성
        index=False  # DataFrame 인덱스는 삽입하지 않음
    )

    
    

def three_lst(indicator_table):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_3.0')  
   
    pg_engine = pg_hook.get_sqlalchemy_engine()

    random_id3 = []

    for i in range(10):
        type = ['payment','refund']
        random_id3.append([random.randrange(1000000,2000000),random.randrange(1000000,2000000),random.choice(type)])
        
    data = pd.DataFrame(random_id3, columns=['id','amount','type'])
    
    data.to_sql(
        name= indicator_table,  # 삽입할 테이블 이름
        con=pg_engine,  # PostgreSQL 연결 엔진
        schema=pg_schema,
        if_exists='append',  # 테이블이 있으면 삭제 후 재생성
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
    f'data-warehouse-test-postgresql-dashboard_demo-incremental_3.0',
    default_args=default_args,
    description='Run query and load result to S3',
    schedule='10 17 * * *',
    tags=['demo','dashboard']
)


add_subject_payment = PythonOperator(
    task_id='add_subject_payment',
    python_callable=two_lst,
    op_kwargs={"indicator_table": "add_subject_payment"},
    provide_context=True,
    dag=dag
)

change_new_tutoring = PythonOperator(
    task_id='change_new_tutoring',
    python_callable=one_lst,
    op_kwargs={"indicator_table": "change_new_tutoring"},
    provide_context=True,
    dag=dag
)

change_pause_tutoring = PythonOperator(
    task_id='change_pause_tutoring',
    python_callable=one_lst,
    op_kwargs={"indicator_table": "change_pause_tutoring"},
    provide_context=True,
    dag=dag
)

change_payment = PythonOperator(
    task_id='change_payment',
    python_callable=three_lst,
    op_kwargs={"indicator_table": "change_payment"},
    provide_context=True,
    dag=dag
)

exeperience_student = PythonOperator(
    task_id='exeperience_student',
    python_callable=one_lst,
    op_kwargs={"indicator_table": "exeperience_student"},
    provide_context=True,
    dag=dag
)

experience_tutoring = PythonOperator(
    task_id='experience_tutoring',
    python_callable=one_lst,
    op_kwargs={"indicator_table": "experience_tutoring"},
    provide_context=True,
    dag=dag
)

extended_payment_after_4month = PythonOperator(
    task_id='extended_payment_after_4month',
    python_callable=two_lst,
    op_kwargs={"indicator_table": "extended_payment_after_4month"},
    provide_context=True,
    dag=dag
)

extended_payment_before_first_round = PythonOperator(
    task_id='extended_payment_before_first_round',
    python_callable=two_lst,
    op_kwargs={"indicator_table": "extended_payment_before_first_round"},
    provide_context=True,
    dag=dag
)

extended_payment_less_then_4month = PythonOperator(
    task_id='extended_payment_less_then_4month',
    python_callable=two_lst,
    op_kwargs={"indicator_table": "extended_payment_less_then_4month"},
    provide_context=True,
    dag=dag
)

first_payment = PythonOperator(
    task_id='first_payment',
    python_callable=two_lst,
    op_kwargs={"indicator_table": "first_payment"},
    provide_context=True,
    dag=dag
)

leave_student = PythonOperator(
    task_id='leave_student',
    python_callable=one_lst,
    op_kwargs={"indicator_table": "leave_student"},
    provide_context=True,
    dag=dag
)

new_student = PythonOperator(
    task_id='new_student',
    python_callable=one_lst,
    op_kwargs={"indicator_table": "new_student"},
    provide_context=True,
    dag=dag
)

new_tutoring = PythonOperator(
    task_id='new_tutoring',
    python_callable=one_lst,
    op_kwargs={"indicator_table": "new_tutoring"},
    provide_context=True,
    dag=dag
)

pause_student = PythonOperator(
    task_id='pause_student',
    python_callable=one_lst,
    op_kwargs={"indicator_table": "pause_student"},
    provide_context=True,
    dag=dag
)

pause_tutoring = PythonOperator(
    task_id='pause_tutoring',
    python_callable=one_lst,
    op_kwargs={"indicator_table": "pause_tutoring"},
    provide_context=True,
    dag=dag
)

reactive_payment = PythonOperator(
    task_id='reactive_payment',
    python_callable=two_lst,
    op_kwargs={"indicator_table": "reactive_payment"},
    provide_context=True,
    dag=dag
)

reactive_student = PythonOperator(
    task_id='reactive_student',
    python_callable=one_lst,
    op_kwargs={"indicator_table": "reactive_student"},
    provide_context=True,
    dag=dag
)

refund_after_4month = PythonOperator(
    task_id='refund_after_4month',
    python_callable=two_lst,
    op_kwargs={"indicator_table": "refund_after_4month"},
    provide_context=True,
    dag=dag
)

refund_before_first_round = PythonOperator(
    task_id='refund_before_first_round',
    python_callable=two_lst,
    op_kwargs={"indicator_table": "refund_before_first_round"},
    provide_context=True,
    dag=dag
)

refund_less_then_4month = PythonOperator(
    task_id='refund_less_then_4month',
    python_callable=two_lst,
    op_kwargs={"indicator_table": "refund_less_then_4month"},
    provide_context=True,
    dag=dag
)

regular_student = PythonOperator(
    task_id='regular_student',
    python_callable=one_lst,
    op_kwargs={"indicator_table": "regular_student"},
    provide_context=True,
    dag=dag
)

regular_tutoring = PythonOperator(
    task_id='regular_tutoring',
    python_callable=one_lst,
    op_kwargs={"indicator_table": "regular_tutoring"},
    provide_context=True,
    dag=dag
)

add_subject_payment >> regular_tutoring >> regular_student >> refund_less_then_4month >> refund_before_first_round >> refund_after_4month >> reactive_student >> reactive_payment >> pause_tutoring >> pause_student >> new_tutoring >> new_student >> leave_student >> first_payment >> extended_payment_less_then_4month >> extended_payment_before_first_round >> extended_payment_after_4month >> experience_tutoring >> exeperience_student >> change_payment >> change_pause_tutoring >> change_new_tutoring >> add_subject_payment