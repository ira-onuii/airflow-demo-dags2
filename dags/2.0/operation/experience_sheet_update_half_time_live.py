from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pendulum import timezone
import experience_live_query

KST = timezone("Asia/Seoul")


raw_query = experience_live_query.raw_sheet_query
monitoring_query = experience_live_query.monitoring_sheet_query
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
    sheet = client.open_by_key('1U_u02aApyTEyV5ygZ0r4KATawxjq3yskD3c06C49dkU').worksheet(sheet_name) 
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
    sheet.update("B5:AE", raw_df.values.tolist(), value_input_option="USER_ENTERED")



def update_monitoring_sheet_query_result(dataframe):
    mointoring_df = dataframe.copy()
    
    # 전부 문자열로 변환 (Timestamp 포함)
    mointoring_df = mointoring_df.astype(str)

    # 결측/특수문자열을 빈칸으로 통일
    mointoring_df = mointoring_df.replace({
        "NaT": "",
        "nan": "",
        "NaN": "",
        "None": "",
        "<NA>": "",
        "inf": "",
        "-inf": "",
    })

    sheet = google_conn(sheet_name='모니터링')
    #sheet.batch_clear(["B6:AI"])
    sheet.update("B5:O", mointoring_df.values.tolist(), value_input_option="USER_ENTERED")



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



def run_raw_query():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = raw_query
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, trino_engine)
    return df


def run_monitoring_query():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = monitoring_query
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, trino_engine)
    return df


def run_operation_query():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = operation_qeury
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, trino_engine)
    return df





def update_raw_query_result():
    update_raw_sheet_query_result(dataframe=run_raw_query())

def update_monitoring_query_result():
    update_monitoring_sheet_query_result(dataframe=run_monitoring_query())

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
    dag_id='experience_query_sheet_update_dag_live_half_time',
    default_args=default_args,
    schedule_interval='30 18 * * *',
    catchup=False,
    tags=['2.0', 'operation', 'experience','live'],
) as dag:

    upload_raw_query_result = PythonOperator(
        task_id='upload_raw_query_result',
        python_callable=update_raw_query_result,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )

    upload_monitoring_query_result = PythonOperator(
        task_id='upload_monitoring_query_result',
        python_callable=update_monitoring_query_result,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )

    upload_operation_query_result = PythonOperator(
        task_id='upload_operation_query_result',
        python_callable=update_operation_query_result,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )


    
    upload_raw_query_result >>  upload_monitoring_query_result >> upload_operation_query_result
