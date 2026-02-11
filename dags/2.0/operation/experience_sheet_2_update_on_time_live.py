import sys
import os

# 현재 파일이 있는 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pendulum import timezone
import experience_live_query

KST = timezone("Asia/Seoul")



operation_qeury = experience_live_query.operation_sheet_query


# 구글 인증 설정
def authorize_gspread():
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        '/opt/airflow/gcp/pj_appscript.json', scope
    )
    client = gspread.authorize(creds)
    return client


def google_conn(sheet_name):
    client = authorize_gspread()
    sheet = client.open_by_key('1Ni0WyPbLvoY6ReRNoyabMSCgwQLR6ORmG94Kk_Jga-U').worksheet(sheet_name) 
    #테스트 : 1htuBC0kD-o1B8_JjYW81fEJ6WDr7_Ox-2mVJtQGsePQ 
    #라이브 : 1JN1V9SFmtIASDplAERz3oDtRhgZgAauqtjg9TbSVHg0
    return sheet









def update_operation_sheet_query_result(dataframe):
    opertation_df = dataframe.copy()
    
    # 전부 문자열로 변환 (Timestamp 포함)
    opertation_df = opertation_df.astype(str)

    # 결측/특수문자열을 빈칸으로 통일
    opertation_df = opertation_df.replace({
        "NaT": "",
        "nan": "",
        "NaN": "",
        "None": "",
        "<NA>": "",
        "inf": "",
        "-inf": "",
    })

    sheet = google_conn(sheet_name='[학생/학부모] 책임 매칭 제도')
    #sheet.batch_clear(["B6:AI"])
    sheet.update("B6:O", opertation_df.values.tolist(), value_input_option="USER_ENTERED")






def run_operation_query():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = operation_qeury
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, trino_engine)
    return df







def update_operation_query_result():
    update_operation_sheet_query_result(dataframe=run_operation_query())




# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, tzinfo=KST),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='experience_query_sheet_2_update_dag_live_on_time',
    default_args=default_args,
    schedule_interval='0 9,11,14,17 * * *',
    catchup=False,
    tags=['2.0', 'operation', 'experience','live'],
) as dag:



    upload_operation_query_result = PythonOperator(
        task_id='upload_operation_query_result',
        python_callable=update_operation_query_result,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )


    
    upload_operation_query_result
