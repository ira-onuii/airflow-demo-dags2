

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from airflow.utils.dates import days_ago
from pendulum import timezone


KST = timezone("Asia/Seoul")

new_column_list = ['start_date', 'lecture_vt_no', 'student_user_no','tutoring_state']
new_columns_str = ", ".join(f'"{col}"' for col in new_column_list)

pause_column_list = ['end_date', 'lecture_vt_no', 'student_user_no','tutoring_state']
pause_columns_str = ", ".join(f'"{col}"' for col in pause_column_list)

def authorize_gspread():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/opt/airflow/gcp/pj_appscript.json', scope) #/opt/airflow/gcp/pj_appscript.json
    client = gspread.authorize(creds)
    return client

def open_worksheet(sheet_name: str):
    client = authorize_gspread()
    return client.open_by_key('1mqCj5Pq0w-vtUr2MjuTqo3AI_gW2puFyak_NZF8wvME').worksheet(sheet_name)



def filter_today_new_list():
    today = (datetime.now() - timedelta(days=1)).strftime("%Y. %m. %d")
    sheet = open_worksheet('신규 수업 명단')
    col_range = "B1:E"
    col_values = sheet.get(col_range)
    df = pd.DataFrame(col_values[1:], columns=col_values[0])
    df_today = df[df["추가 일자"] == today]
    df_today = pd.DataFrame(columns=new_column_list) 
    list = df_today['lecture_vt_no']
    return df_today,list



def filter_today_pause_list():
    today = (datetime.now() - timedelta(days=1)).strftime("%Y. %m. %d")
    sheet = open_worksheet('중단 수업 명단')
    col_range = "B1:E"
    col_values = sheet.get(col_range)
    df = pd.DataFrame(col_values[1:], columns=col_values[0])
    df_today = df[df["추가 일자"] == today]
    df_today = pd.DataFrame(columns=pause_column_list) 
    list = df_today['lecture_vt_no']
    return df_today,list


def merge_fst_months_new():
    import pandas as pd
    from airflow.providers.trino.hooks.trino import TrinoHook

    # Trino 연결
    trino_engine = TrinoHook(trino_conn_id='trino_conn').get_sqlalchemy_engine()

    # 함수는 한 번만 호출해서 일관성 보장
    new_df, ids = filter_today_new_list()

    # ids를 리스트로 정규화
    if hasattr(ids, "tolist"):
        ids = ids.tolist()

    # lecture_vt_no가 문자열이라면 모두 따옴표 감싸기 (숫자라면 따옴표 빼도 OK)
    def sql_literal(v):
        s = str(v)
        return "'" + s.replace("'", "''") + "'"   # 작은따옴표 이스케이프

    in_list = ", ".join(sql_literal(v) for v in ids)

    # 혹시라도 빈 경우 안전 처리 (inner join이면 빈 DF 반환)
    if not in_list:
        return new_df.iloc[0:0].assign(lecture_vt_no=pd.Series(dtype=new_df.get("lecture_vt_no", pd.Series(dtype="object")).dtype))

    query = f"""
        WITH glvt AS (
            SELECT 
                lecture_vt_no, MAX(glvt.min_payment_no) AS min_payment_no
            FROM data_warehouse.raw_data.group_lvt glvt
            WHERE glvt.lecture_vt_no IN ({in_list})
            GROUP BY lecture_vt_no
        ),
        fst_months AS (
            SELECT glvt.lecture_vt_no, th.months AS fst_months
            FROM glvt
            INNER JOIN mysql.onuei.payment p ON glvt.min_payment_no = p.payment_no 
            INNER JOIN mysql.onuei.tteok_ham th ON p.tteok_ham_no = th.tteok_ham_no 
        )
        SELECT * FROM fst_months
    """

    result = pd.read_sql(query, con=trino_engine)
    merge_new_result = new_df.merge(result, on='lecture_vt_no', how='inner')
    return merge_new_result




def merge_fst_months_pause():
    import pandas as pd
    from airflow.providers.trino.hooks.trino import TrinoHook

    # Trino 연결
    trino_engine = TrinoHook(trino_conn_id='trino_conn').get_sqlalchemy_engine()

    # 함수는 한 번만 호출해서 일관성 보장
    pause_df, ids = filter_today_pause_list()

    # ids를 리스트로 정규화
    if hasattr(ids, "tolist"):
        ids = ids.tolist()

    # lecture_vt_no가 문자열이라면 모두 따옴표 감싸기 (숫자라면 따옴표 빼도 OK)
    def sql_literal(v):
        s = str(v)
        return "'" + s.replace("'", "''") + "'"   # 작은따옴표 이스케이프

    in_list = ", ".join(sql_literal(v) for v in ids)

    # 혹시라도 빈 경우 안전 처리 (inner join이면 빈 DF 반환)
    if not in_list:
        return pause_df.iloc[0:0].assign(lecture_vt_no=pd.Series(dtype=pause_df.get("lecture_vt_no", pd.Series(dtype="object")).dtype))

    query = f"""
        WITH glvt AS (
            SELECT 
                lecture_vt_no, MAX(glvt.min_payment_no) AS min_payment_no
            FROM data_warehouse.raw_data.group_lvt glvt
            WHERE glvt.lecture_vt_no IN ({in_list})
            GROUP BY lecture_vt_no
        ),
        fst_months AS (
            SELECT glvt.lecture_vt_no, th.months AS fst_months
            FROM glvt
            INNER JOIN mysql.onuei.payment p ON glvt.min_payment_no = p.payment_no 
            INNER JOIN mysql.onuei.tteok_ham th ON p.tteok_ham_no = th.tteok_ham_no 
        )
        SELECT * FROM fst_months
    """

    result = pd.read_sql(query, con=trino_engine)
    merge_pause_result = pause_df.merge(result, on='lecture_vt_no', how='inner')
    return merge_pause_result




def load_new_result():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    # postgresql 연결
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_2.0')  
    pg_engine = pg_hook.get_sqlalchemy_engine()

    fin_new_result = merge_fst_months_new().to_sql(
        name='new_lecture',
        con=pg_engine,
        schema='kpis',
        if_exists='append',
        index=False
    )

    return fin_new_result


def load_pause_result():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    # postgresql 연결
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_2.0')  
    pg_engine = pg_hook.get_sqlalchemy_engine()

    fin_new_result = merge_fst_months_pause().to_sql(
        name='pause_lecture',
        con=pg_engine,
        schema='kpis',
        if_exists='append',
        index=False
    )

    return fin_new_result


default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'KM_calculator_daily_actuals',
    default_args=default_args,
    description='Run query and load result to S3',
    schedule='10 17 * * *',
    tags=['2.0','KM_calculator'],
    catchup=False
)


load_new_lecture = PythonOperator(
    task_id='load_new_lecture',
    python_callable=load_new_result,
    provide_context=True,
    dag=dag
)

load_pause_lecture = PythonOperator(
    task_id='load_pause_lecture',
    python_callable=load_pause_result,
    provide_context=True,
    dag=dag
)


load_new_lecture >> load_pause_lecture












    
