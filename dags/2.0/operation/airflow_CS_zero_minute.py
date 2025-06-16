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
    sheet = client.open_by_key('1xyHTIVLciMWophBRA4fE3aFP1qxncMOOWP4vU8Gx6C8').worksheet(sheet_name)
    return sheet

# 쿼리 결과를 시트에 업로드
# def update_google_sheet(range_start_cell, dataframe):
#     sheet = google_conn(sheet_name='과외신청서 미작성 CS 확인용')

    
#     # Pandas DF를 시트에 쓰기 위해 리스트 변환
#     values = [dataframe.columns.tolist()] + dataframe.values.tolist()
#     sheet.update(range_start_cell, values)

#빈 행 탐색
def found_blank():
    sheet = google_conn(sheet_name='2회 0분 마치기')
    # 대상 열 전체 읽기 (예: 'A2:A')
    col_range = "A2:F"
    col_values = sheet.get(col_range)
    
    # 빈 행 찾기: 값이 없으면 빈 리스트거나 빈 문자열
    first_empty_row = 2  # A2부터 시작
    for i, row in enumerate(col_values, start=2):
        if not row or not row[0]:  # 빈 값이면
            first_empty_row = i
            break
    else:
        # 모두 채워졌다면 그 아래 행부터 입력
        first_empty_row = len(col_values) + 2

    # 업데이트할 셀 범위
    range_start_cell = f"A{first_empty_row}"
    return range_start_cell


# 쿼리 결과 append
def update_google_sheet_append_by_column(range_start_cell, dataframe):
    sheet = google_conn(sheet_name='2회 0분 마치기')

    # 데이터프레임을 시트에 쓰기 위한 리스트로 변환
    sheet.update(range_start_cell, dataframe)




def run_query():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = '''
with lvc2 as (
select lvc.lecture_vt_no, lvc.lecture_cycle_no
	from mysql.onuei.lecture_vt_cycles lvc
	where lvc.durations = 0
	and lvc.cycle_state = 'DONE'
	and date_format(lvc.update_datetime,'%Y-%m%-%d') = date_format((now() - interval '1' day),'%Y-%m-%d')
),
list as (
select lvc.lecture_vt_no, lvc.lecture_cycle_no, lag(lvc.durations) over(partition by lecture_vt_no order by req_datetime asc) as before_duration, lvc.durations, lvc.update_datetime 
	from mysql.onuei.lecture_vt_cycles lvc
	where lvc.lecture_vt_no in (select lvc2.lecture_vt_No from lvc2)
	-- and date_format(lvc.update_datetime,'%Y-%m%-%d') = '2025-06-15'
),
list2 as (
select lecture_vt_No
	from list
	where list.before_duration = 0
	and list.durations = 0
	and date_format(list.update_datetime,'%Y-%m%-%d') = date_format((now() - interval '1' day),'%Y-%m-%d')
	group by lecture_vt_No
)
-- select * from list2
,
u as (
select u.user_no, u.name
	from mysql.onuei."user" u
),
meta_data as (
select lvt.lecture_vt_no, ttn.name as subject, lvt.student_user_no, u.name as student_name, ltvt.teacher_user_no, u2.name as teacher_name
	from mysql.onuei.lecture_video_tutoring lvt
	inner join mysql.onuei.lecture_teacher_vt ltvt on lvt.lecture_vt_no = ltvt.lecture_vt_no
	inner join mysql.onuei.term_taxonomy_name ttn on lvt.lecture_subject_id = ttn.term_taxonomy_id 
	inner join u on lvt.student_user_no = u.user_no 
	inner join u u2 on ltvt.teacher_user_no = u2.user_No
	where ltvt.teacher_vt_status = 'ASSIGN'
)
select date_format(now(),'%Y-%m-%d') as today, student_name, student_user_No, teacher_name, teacher_user_No, subject
	from list2 
	inner join meta_data on list2.lecture_vt_No = meta_data.lecture_vt_No

'''
    trino_hook = TrinoHook(trino_conn_id='trino_conn')   

    # SQLAlchemy Engine 생성
    trino_engine = trino_hook.get_sqlalchemy_engine()
    CS_list = pd.read_sql(query, trino_engine)
    df = CS_list.values.tolist()
    return df

# DAG 내에서 사용될 함수
def upload_daily_data():
    # Trino 연결
    # 실제 쿼리 → Pandas DF 처리
    df = run_query()
    update_google_sheet_append_by_column(dataframe=df, range_start_cell=found_blank())


# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, tzinfo=KST),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='CS_zero_done_google_sheet_update_dag',
    default_args=default_args,
    schedule_interval='0 9 * * 1,3,5',  # 매일 오전 10시
    catchup=False,
    tags=['2.0', 'operation', 'CS'],
) as dag:

    upload_daily = PythonOperator(
        task_id='upload_daily_data',
        python_callable=upload_daily_data,
    )



    upload_daily
