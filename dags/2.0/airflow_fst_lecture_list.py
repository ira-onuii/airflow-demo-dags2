import sys
import os

# 현재 파일이 있는 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import fst_lecture_query
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy.orm import Session
from airflow.utils.session import provide_session   
import pendulum
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from jinja2 import Template
import pytz




date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

user_filename = 'list.csv'



# S3 연결 및 csv 파일 저장
def fst_lecture_save_to_s3_with_hook(data, bucket_name, file_name):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)

    hook = S3Hook(aws_conn_id='conn_S3')
    hook.load_string(csv_buffer.getvalue(), key=f"{file_name}", bucket_name=bucket_name, replace=True)



# 마지막 dag 성공시점 추출
@provide_session
def get_latest_successful_interval(dag_id, current_execution_date, session: Session = None):
    from airflow.models import DagRun
    from airflow.utils.state import State
    # 현재 실행보다 이전에 성공한 DAG run을 찾기
    previous_success = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date < current_execution_date,
            DagRun.state == State.SUCCESS
        )
        .order_by(DagRun.execution_date.desc())
        .first()
    )

    if not previous_success:
        raise ValueError("No successful DAG run found before current execution")

    return previous_success.data_interval_end



# 리스트 추출
def schedule_list_update(**context):
    from airflow.providers.trino.hooks.trino import TrinoHook

    # 시간 추출
    dag_id = context['dag'].dag_id

    current_execution_date = context['execution_date']
    utc_end = context['data_interval_end']

    last_success_end = get_latest_successful_interval(dag_id, current_execution_date)

    # KST로 변환
    kst = pytz.timezone('Asia/Seoul')
    start_kst = last_success_end.astimezone(kst)
    end_kst = utc_end.astimezone(kst)

    # timestamp 포멧 변환
    start_str = start_kst.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_kst.strftime('%Y-%m-%d %H:%M:%S')
    print("▶️ Query interval:", start_str, "~", end_str)

    # 쿼리 렌더링 (쿼리에 실행시점 적용)
    lvs_query = Template(fst_lecture_query.lvs_query_template).render(
        data_interval_start=start_str,
        data_interval_end=end_str
    )
    print("▶️ Used in SQL:", lvs_query)

    lvc_query = Template(fst_lecture_query.lvc_query_template).render(
        data_interval_start=start_str,
        data_interval_end=end_str
    )
    print("▶️ Used in SQL:", lvc_query)
    # Trino 연결
    trino_hook = TrinoHook(trino_conn_id='trino_conn')   

    # SQLAlchemy Engine 생성
    trino_engine = trino_hook.get_sqlalchemy_engine()


    # raw_data 불러오기
    lvs = pd.read_sql(lvs_query, trino_engine)
    t = pd.read_sql(fst_lecture_query.t_query, trino_engine)
    warmup1 = pd.read_sql('''SELECT 1 FROM mysql.onuei.payment WHERE false''', trino_engine)
    p = pd.read_sql(fst_lecture_query.p_query, trino_engine)
    mlvt = pd.read_sql(fst_lecture_query.mlvt_query, trino_engine)
    warmup2 = pd.read_sql('''SELECT 1 FROM mysql.onuei.lecture_vt_cycles WHERE false''', trino_engine)
    lvc = pd.read_sql(lvc_query, trino_engine)

    # lvt 별 최초 결제
    p['prn'] = p.sort_values(by = ['payment_regdate'], ascending = True).groupby(['lecture_vt_No']).cumcount()+1
    p_1 = p[p['prn'] == 1]
    p_result = p_1[p_1['payment_regdate'] >= '2024-11-01 00:00:00']

    # lvt 별 최초 매칭
    mlvt['mrn'] = mlvt.sort_values(by = ['matchedat'], ascending = True).groupby(['lecture_vt_No']).cumcount()+1
    mlvt_result = mlvt[mlvt['mrn'] == 1]


    # schedule & cycle  Inner Join
    schedule_list = pd.merge(lvs, lvc, on='lecture_cycle_No', how='inner')
    print(schedule_list)

    # raw data 병합
    df = mlvt_result.merge(schedule_list, on=["lecture_vt_No", "teacher_user_No"], how="inner") \
            .merge(p_result, on="lecture_vt_No", how="inner") \
            .merge(t, left_on="teacher_user_No", right_on='user_No', how="inner") \

    meta_data = pd.read_sql(fst_lecture_query.meta_data_query, trino_engine)

    df_meta = df.merge(meta_data, on='lecture_vt_No', how='left') \
        .sort_values(by=["lecture_vt_No"])
    print(df_meta.columns)
    df_meta = df_meta[["lecture_vt_No", "subject", "student_user_No", "student_name_y","teacher_user_No","teacher_name","page_call_room_id","tutoring_datetime", "tteok_ham_type", "durations"]]
    print(df_meta)
    return df_meta


# 결과 정제 및 S3 저장
def fst_lecture_save_results_to_s3(**context):
    # 컬럼 정렬
    column_names = ["lecture_vt_No", "subject", "student_user_No", "student_name_y","teacher_user_No","teacher_name", 'rn',"page_call_room_id","tutoring_datetime", "tteok_ham_type", "durations"]
    hook = S3Hook(aws_conn_id='conn_S3')
    # S3에 있는 기존 파일 불러오기
    s3_obj = hook.get_key(key=user_filename, bucket_name='seoltab-datasource')
    content = s3_obj.get()['Body'].read().decode('utf-8')
    print('S3 connected')
    try:
        existing_df = pd.read_csv(StringIO(content))
    except FileNotFoundError:
        print("파일이 존재하지 않습니다. 새로 생성합니다.")
        existing_df = pd.DataFrame()
     
    print(existing_df)
    # 최근 결과 (쿼리 결과) 가져오기
    query_results = context['ti'].xcom_pull(task_ids='fst_lecture_run_query')
    query_results = pd.DataFrame(query_results, columns=column_names)
    query_results['student_name'] = query_results['student_name_y']
    # 기존 파일, 최근 결과 병합
    updated_df = pd.concat([existing_df, query_results], ignore_index=True)
    # 날짜 타입으로 변환 (안 되는 값은 NaT 처리)
    updated_df['tutoring_datetime'] = pd.to_datetime(updated_df['tutoring_datetime'], errors='coerce')
    updated_df['page_call_room_id'] = updated_df['page_call_room_id'].str.strip()
    updated_df = updated_df.drop_duplicates(subset=['page_call_room_id'], keep='last')
    # 회차열 생성 및 정렬
    updated_df['rn'] = updated_df.sort_values(by = ['tutoring_datetime'], ascending = True).groupby(['lecture_vt_No']).cumcount()+1
    updated_df['rn'] = updated_df['rn'].astype(int)
    updated_df = updated_df[["lecture_vt_No", "subject", "student_user_No", "student_name","teacher_user_No","teacher_name", 'rn',"page_call_room_id","tutoring_datetime", "tteok_ham_type", "durations"]]
    updated_df.sort_values(by=["lecture_vt_No",'rn'], ascending=[True, True])
    fst_lecture_save_to_s3_with_hook(updated_df, 'seoltab-datasource', user_filename)




default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'data-warehouse-test-tirno-AI_2.0',
    default_args=default_args,
    description='Run query and load result to S3',
    start_date=datetime(2024, 11, 13, 6, 25),
    tags=["2.0"],
    schedule='*/25 * * * *',
    catchup=False
)



fst_lecture_run_query = PythonOperator(
    task_id='fst_lecture_run_query',
    python_callable=schedule_list_update,
    provide_context=True,
    retries=5,
    retry_delay=timedelta(seconds=2),
    dag=dag,
)


fst_lecture_save_to_s3_task = PythonOperator(
    task_id='fst_lecture_list_save_to_s3',
    python_callable=fst_lecture_save_results_to_s3,
    provide_context=True,
    dag=dag,
)






# dbt_run = BashOperator(
#     task_id='dbt_run',
#     bash_command='dbt run --profiles-dir /opt/airflow/dbt_project/.dbt --project-dir /opt/airflow/dbt_project --models /opt/airflow/dbt_project/models/pg_active_lecture/active_lecture.sql',
# )

fst_lecture_run_query >> fst_lecture_save_to_s3_task 


