import sys
import os

# 현재 파일이 있는 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import fst_lecture_query

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import test_query
from jinja2 import Template
import pytz


date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

user_filename = 'list_test.csv'



   
def fst_lecture_save_to_s3_with_hook(data, bucket_name, file_name):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)

    hook = S3Hook(aws_conn_id='conn_S3')
    hook.load_string(csv_buffer.getvalue(), key=f"{file_name}", bucket_name=bucket_name, replace=True)




def schedule_list_update(**context):
    from airflow.providers.trino.hooks.trino import TrinoHook

    # 1. 시간 추출 + KST로 변환
    utc_start = context['execution_date']
    utc_end = context['data_interval_end']

    # KST로 변환
    kst = pytz.timezone('Asia/Seoul')
    start_kst = utc_start.astimezone(kst)
    end_kst = utc_end.astimezone(kst)

    # Trino friendly 포맷
    start_str = start_kst.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_kst.strftime('%Y-%m-%d %H:%M:%S')

    # 쿼리 렌더링
    lvs_query = Template(test_query.lvs_query_template).render(
        execution_date=start_str,
        data_interval_end=end_str
    )

    lvc_query = Template(test_query.lvc_query_template).render(
        execution_date=start_str,
        data_interval_end=end_str
    )


    trino_hook = TrinoHook(trino_conn_id='trino_conn')   

    # SQLAlchemy Engine 생성
    trino_engine = trino_hook.get_sqlalchemy_engine()


    # raw_data 불러오기
    lvs = pd.read_sql(lvs_query, trino_engine)
    t = pd.read_sql(test_query.t_query, trino_engine)
    warmup1 = pd.read_sql('''SELECT 1 FROM mysql.onuei.payment WHERE false''', trino_engine)
    p = pd.read_sql(test_query.p_query, trino_engine)
    mlvt = pd.read_sql(test_query.mlvt_query, trino_engine)
    warmup2 = pd.read_sql('''SELECT 1 FROM mysql.onuei.lecture_vt_cycles WHERE false''', trino_engine)
    lvc = pd.read_sql(lvc_query, trino_engine)

    # lvt 별 최초 결제
    p['rn'] = p.sort_values(by = ['payment_regdate'], ascending = True).groupby(['lecture_vt_No']).cumcount()+1
    p_1 = p[p['rn'] == 1]
    p_result = p_1[p_1['payment_regdate'] >= '2024-11-01 00:00:00']

    # lvt 별 최초 매칭
    mlvt['rn'] = mlvt.sort_values(by = ['matchedat'], ascending = True).groupby(['lecture_vt_No']).cumcount()+1
    mlvt_result = mlvt[mlvt['rn'] == 1]


    # schedule & cycle  Inner Join
    schedule_list = pd.merge(lvs, lvc, on='lecture_cycle_No', how='inner')

    # 최근 결과
    df = mlvt_result.merge(schedule_list, on=["lecture_vt_No", "teacher_user_No"], how="inner") \
            .merge(p_result, on="lecture_vt_No", how="inner") \
            .merge(t, left_on="teacher_user_No", right_on='user_No', how="inner") \

    meta_data = pd.read_sql(test_query.meta_data_query, trino_engine)

    df_meta = df.merge(meta_data, on='lecture_vt_No', how='left') \
        .sort_values(by=["lecture_vt_No"])
    
    return df_meta



def fst_lecture_save_results_to_s3(**context):
    column_names = ["lecture_vt_No", "subject", "student_user_No", "student_name","teacher_user_No","teacher_name", 'schedule_rn',"page_call_room_id","tutoring_datetime"]
    hook = S3Hook(aws_conn_id='conn_S3')
    s3_obj = hook.get_key(key='list_test.csv', bucket_name='seoltab-datasource')
    content = s3_obj.get()['Body'].read().decode('utf-8')
    print('S3 connected')
    try:
        existing_df = pd.read_csv(StringIO(content))
    except FileNotFoundError:
        print("파일이 존재하지 않습니다. 새로 생성합니다.")
        existing_df = pd.DataFrame()
    
    print(existing_df)
    query_results = context['ti'].xcom_pull(task_ids='fst_lecture_run_query_test')
    query_results = pd.DataFrame(query_results, columns=column_names)
    updated_df = pd.concat([existing_df, query_results], ignore_index=True)
    #updated_df = updated_df.drop_duplicates(subset=['page_call_room_id'], keep='last')
    updated_df['tutoring_datetime'] = pd.to_datetime(updated_df['tutoring_datetime'], errors='coerce')
    updated_df['schedule_rn'] = updated_df.sort_values(by = ['tutoring_datetime'], ascending = True).groupby(['lecture_vt_No']).cumcount()+1
    updated_df.sort_values(by=["lecture_vt_No",'schedule_rn'], ascending=[True, True])
    fst_lecture_save_to_s3_with_hook(updated_df, 'seoltab-datasource', 'list_test.csv')





default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'tirno-AI_2.0_test',
    default_args=default_args,
    description='Run query and load result to S3',
    start_date=datetime(2024, 11, 13, 6, 25),
    tags=["2.0"],
    schedule='10 17 * * *',
    catchup=False
)



#user
fst_lecture_run_query = PythonOperator(
    task_id='fst_lecture_run_query_test',
    python_callable=schedule_list_update,
    provide_context=True,
    retries=2,
    retry_delay=timedelta(seconds=2),
    dag=dag,
)


fst_lecture_save_to_s3_task = PythonOperator(
    task_id='fst_lecture_list_save_to_s3_test',
    python_callable=fst_lecture_save_results_to_s3,
    provide_context=True,
    dag=dag,
)






# dbt_run = BashOperator(
#     task_id='dbt_run',
#     bash_command='dbt run --profiles-dir /opt/airflow/dbt_project/.dbt --project-dir /opt/airflow/dbt_project --models /opt/airflow/dbt_project/models/pg_active_lecture/active_lecture.sql',
# )

fst_lecture_run_query >> fst_lecture_save_to_s3_task 


