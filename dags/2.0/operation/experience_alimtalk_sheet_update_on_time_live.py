from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pendulum import timezone
import experience_live_query

KST = timezone("Asia/Seoul")


raw_query = experience_live_query.alimtalk_sheet_query



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
    sheet = client.open_by_key('1tQ9QeLEbHXX-yzsouJbuahcyOTjFJb0-Sj9gkJ5eGpA').worksheet(sheet_name) 
    #테스트 : 1htuBC0kD-o1B8_JjYW81fEJ6WDr7_Ox-2mVJtQGsePQ 
    #라이브 : 1JN1V9SFmtIASDplAERz3oDtRhgZgAauqtjg9TbSVHg0
    return sheet





def update_raw_sheet_query_result(dataframe):
    raw_df = dataframe.copy()
    
    # 전부 문자열로 변환 (Timestamp 포함)
    raw_df = raw_df.astype(str)

    # 결측/특수문자열을 빈칸으로 통일
    raw_df = raw_df.replace({
        "NaT": "",
        "nan": "",
        "NaN": "",
        "None": "",
        "<NA>": "",
        "inf": "",
        "-inf": "",
    })

    sheet = google_conn(sheet_name='raw')
    #sheet.batch_clear(["B6:AI"])
    sheet.update("B4:G", raw_df.values.tolist(), value_input_option="USER_ENTERED")






def run_raw_query():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = raw_query
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, trino_engine)
    return df








def update_raw_query_result():
    update_raw_sheet_query_result(dataframe=run_raw_query())






# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, tzinfo=KST),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='experience_query_sheet_update_dag_live_on_time',
    default_args=default_args,
    schedule_interval='0 18 * * *',
    catchup=False,
    tags=['2.0', 'operation', 'experience','live'],
) as dag:

    upload_raw_query_result = PythonOperator(
        task_id='upload_raw_query_result',
        python_callable=update_raw_query_result,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )


    
    upload_raw_query_result
