from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pendulum import timezone

KST = timezone("Asia/Seoul")

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
    sheet = client.open_by_key('1LJd8uXrXabjPhUNwBrs43N79Z6-QtTu3Wq3CksiWEnI').worksheet(sheet_name)
    return sheet

# 쿼리 결과를 시트에 업로드
def update_google_sheet(delete_range,range_start_cell, dataframe):
    sheet = google_conn(sheet_name='재고RAW')

    
    sheet.batch_clear([delete_range])
    # Pandas DF를 시트에 쓰기 위해 리스트 변환
    values = [dataframe.columns.tolist()] + dataframe.values.tolist()
    sheet.update(range_start_cell, values)

def run_query():
    from airflow.providers.trino.hooks.trino import TrinoHook
    trino_hook = TrinoHook(trino_conn_id='trino_conn')   

    # SQLAlchemy Engine 생성
    trino_engine = trino_hook.get_sqlalchemy_engine()
    stock_count = pd.read_sql("select * from mysql.onuei.stock_count", trino_engine)
    df = stock_count
    return df

# DAG 내에서 사용될 함수
def upload_daily_data():
    # Trino 연결
    # 실제 쿼리 → Pandas DF 처리
    df = run_query()
    update_google_sheet(delete_range='C2:P40', range_start_cell='C2', dataframe=df)

def upload_monthly_data():
    df = run_query()
    update_google_sheet(delete_range='C42:P80', range_start_cell='C42', dataframe=df)

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, tzinfo=KST),
    'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='google_sheet_update_dag',
    default_args=default_args,
    schedule_interval='0 7 * * *',  # 매일 오전 7시
    catchup=False,
    tags=['2.0', 'operation', 'stock_count'],
) as dag:

    upload_daily = PythonOperator(
        task_id='upload_daily_data',
        python_callable=upload_daily_data,
    )

    upload_monthly = PythonOperator(
        task_id='upload_monthly_data',
        python_callable=upload_monthly_data,
    )

    # 매월 말일에만 실행되도록 조건 분기
    def should_run_on_last_day(**context):
        today = context['ds']
        date_obj = datetime.strptime(today, '%Y-%m-%d')
        tomorrow = date_obj + timedelta(days=1)
        return tomorrow.day == 1  # 오늘이 말일이면 True

    from airflow.operators.python import ShortCircuitOperator

    monthly_branch = ShortCircuitOperator(
        task_id='check_if_last_day',
        python_callable=should_run_on_last_day,
        provide_context=True,
    )

    upload_daily >> monthly_branch >> upload_monthly
