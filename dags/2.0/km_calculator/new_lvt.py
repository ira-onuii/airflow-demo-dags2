

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from airflow.utils.dates import days_ago
from pendulum import timezone


KST = timezone("Asia/Seoul")

new_column_list = ['start_date', 'lecture_vt_no', 'student_user_no','tutoring_state', 'fst_months']
new_columns_str = ", ".join(f'"{col}"' for col in new_column_list)

pause_column_list = ['end_date', 'lecture_vt_no', 'student_user_no','tutoring_state', 'fst_months']
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
    import pandas as pd
    from datetime import datetime, timedelta

    # 날짜 비교는 문자열 포맷 대신 datetime으로 안전하게 처리
    # 어제 걸 뽑는 게 의도면 -1day 유지, "오늘"이면 timedelta 제거
    target_date = (datetime.now() - timedelta(days=1)).date()

    sheet = open_worksheet('신규 수업 명단')
    col_range = "B1:E"
    col_values = sheet.get(col_range)

    df = pd.DataFrame(col_values[1:], columns=col_values[0])

    # 공백 제거
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # '추가 일자'를 datetime으로 파싱 (시트 값이 '2025. 08. 28'이든 '2025-08-28'이든 대응)
    df['추가 일자'] = pd.to_datetime(df['추가 일자'], errors='coerce').dt.date

    # 날짜 필터
    df_today = df[df['추가 일자'] == target_date].copy()

    # 필요한 컬럼만 선택 (기존에 쓰던 pause_column_list 활용)
    # df_today = df_today[pause_column_list]  # ← 이 줄만 쓰세요. 새 DF로 초기화 금지!

    # ids 추출: 시트가 문자열일 수 있으니 형 변환/결측 제거
    ids = (
        df_today['lecture_vt_No']
        .dropna()
        .map(lambda x: str(x).strip())
        .tolist()
    )

    return df_today, ids




def filter_today_pause_list():
    import pandas as pd
    from datetime import datetime, timedelta

    # 날짜 비교는 문자열 포맷 대신 datetime으로 안전하게 처리
    # 어제 걸 뽑는 게 의도면 -1day 유지, "오늘"이면 timedelta 제거
    target_date = (datetime.now() - timedelta(days=1)).date()

    sheet = open_worksheet('중단 수업 명단')
    col_range = "B1:E"
    col_values = sheet.get(col_range)

    df = pd.DataFrame(col_values[1:], columns=col_values[0])

    # 공백 제거
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # '추가 일자'를 datetime으로 파싱 (시트 값이 '2025. 08. 28'이든 '2025-08-28'이든 대응)
    df['추가 일자'] = pd.to_datetime(df['추가 일자'], errors='coerce').dt.date

    # 날짜 필터
    df_today = df[df['추가 일자'] == target_date].copy()

    # 필요한 컬럼만 선택 (기존에 쓰던 pause_column_list 활용)
    # df_today = df_today[pause_column_list]  # ← 이 줄만 쓰세요. 새 DF로 초기화 금지!

    # ids 추출: 시트가 문자열일 수 있으니 형 변환/결측 제거
    ids = (
        df_today['lecture_vt_No']
        .dropna()
        .map(lambda x: str(x).strip())
        .tolist()
    )

    return df_today, ids



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
            WHERE cast(glvt.lecture_vt_no as varchar) IN ({in_list})
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
    print(query)
    result = pd.read_sql(query, con=trino_engine)
    new_df["lecture_vt_No"] = pd.to_numeric(pause_df["lecture_vt_No"], errors="coerce").astype("Int64")
    result["lecture_vt_no"] = pd.to_numeric(result["lecture_vt_no"], errors="coerce").astype("Int64")
    merge_new_result = new_df.merge(result, left_on='lecture_vt_No', right_on='lecture_vt_no', how='inner')
    print(f'####new_query_result #### {merge_new_result}')
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
        return pause_df.iloc[0:0].assign(lecture_vt_no=pd.Series(dtype=pause_df.get("lecture_vt_No", pd.Series(dtype="object")).dtype))

    query = f"""
        WITH glvt AS (
            SELECT 
                lecture_vt_no, MAX(glvt.min_payment_no) AS min_payment_no
            FROM data_warehouse.raw_data.group_lvt glvt
            WHERE cast(lecture_vt_no as varchar) IN ({in_list})
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
    print(query)
    result = pd.read_sql(query, con=trino_engine)
    pause_df["lecture_vt_No"] = pd.to_numeric(pause_df["lecture_vt_No"], errors="coerce").astype("Int64")
    result["lecture_vt_no"] = pd.to_numeric(result["lecture_vt_no"], errors="coerce").astype("Int64")

    merge_pause_result = pause_df.merge(result, left_on='lecture_vt_No', right_on='lecture_vt_no', how='inner')

    print(f'####pause_query_result #### {merge_pause_result}')
    return merge_pause_result




def load_new_result():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    # postgresql 연결
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_2.0')  
    pg_engine = pg_hook.get_sqlalchemy_engine()
    fin_new_result = merge_fst_months_new()
    fin_new_result = fin_new_result.reindex(column = new_columns_str)
    fin_new_result = fin_new_result.to_sql(
        name='new_lecture',
        con=pg_engine,
        schema='kpis',
        if_exists='replace',
        index=False
    )
    print(f'####new_query_result #### {fin_new_result}')
    return fin_new_result


def load_pause_result():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    # postgresql 연결
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_2.0')  
    pg_engine = pg_hook.get_sqlalchemy_engine()
    fin_pause_result = merge_fst_months_pause()
    fin_pause_result = fin_pause_result.reindex(column = pause_columns_str)
    fin_pause_result = fin_pause_result.to_sql(
        name='pause_lecture',
        con=pg_engine,
        schema='kpis',
        if_exists='replace',
        index=False
    )
    print(f'####new_query_result #### {fin_pause_result}')
    return fin_pause_result


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












    
