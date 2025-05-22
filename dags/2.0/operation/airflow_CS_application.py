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
    sheet = client.open_by_key('1htuBC0kD-o1B8_JjYW81fEJ6WDr7_Ox-2mVJtQGsePQ').worksheet(sheet_name)
    return sheet

# 쿼리 결과를 시트에 업로드
# def update_google_sheet(range_start_cell, dataframe):
#     sheet = google_conn(sheet_name='과외신청서 미작성 CS 확인용')

    
#     # Pandas DF를 시트에 쓰기 위해 리스트 변환
#     values = [dataframe.columns.tolist()] + dataframe.values.tolist()
#     sheet.update(range_start_cell, values)

#빈 행 탐색
def found_blank():
    sheet = google_conn(sheet_name='과외신청서 미작성 CS 확인용')
    # 대상 열 전체 읽기 (예: 'A2:A')
    col_range = "B2:D"
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
    range_start_cell = f"B{first_empty_row}"
    return range_start_cell


# 쿼리 결과 append
def update_google_sheet_append_by_column(range_start_cell, dataframe):
    sheet = google_conn(sheet_name='과외신청서 미작성 CS 확인용')

    # 데이터프레임을 시트에 쓰기 위한 리스트로 변환
    values = [dataframe.columns.tolist()] + dataframe.values.tolist()
    sheet.update(range_start_cell, values)


# def update_google_sheet(range_start_cell, dataframe):
#     sheet = google_conn(sheet_name='과외신청서 미작성 CS 확인용')

    
#     # Pandas DF를 시트에 쓰기 위해 리스트 변환
#     values = [dataframe.columns.tolist()] + dataframe.values.tolist()
#     sheet.update(range_start_cell, values)


def run_query():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = '''
with lvt as (
select lvt.lecture_vt_no,lvt.student_user_no, lvt.tutoring_state,ttn.name as subject, u.name as student_name, if(lvt.reactive_datetime is null,lvt.create_datetime, reactive_datetime) as crda
	from mysql.onuei.lecture_video_tutoring lvt
	inner join mysql.onuei."user" u on lvt.student_user_No = u.user_no 
	inner join mysql.onuei.term_taxonomy_name ttn on lvt.lecture_subject_id = ttn.term_taxonomy_id 
	where u.email_id not like '%onuii%'
	and u.email_id not like '%test%'
	and u.phone_number not like '%00000%'
	and u.phone_number not like '%11111%'
	and u.name not like '%테스트%'
    and u.user_No not in (621888,621889,622732,615701)
	and lvt.student_type in ('PAYED','PAYED_B')
	and lvt.tutoring_state = 'REGISTER'
	and lvt.application_datetime is null
),
p as (
select p.lecture_vt_no, p.student_user_No, p.student_name, p.subject, p.state, p.payment_regdate
	from 
		(select row_number() over(partition by p.lecture_vt_no order by p.payment_regdate asc) as rn 
			,p.payment_regdate, p.state, lvt.*
			from mysql.onuei.payment p
			inner join lvt on (p.lecture_vt_no = lvt.lecture_vt_no and p.payment_regdate >= lvt.crda)
		) p
	where p.rn = 1
	and p.state = '결제완료'
	and date_diff('day', p.payment_regdate, now()) >= 3
)
select p.student_name, p.student_user_No, p.subject
	from p
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
    'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='CS_google_sheet_update_dag',
    default_args=default_args,
    schedule_interval='0 7 * * *',  # 매일 오전 7시
    catchup=False,
    tags=['2.0', 'operation', 'CS'],
) as dag:

    upload_daily = PythonOperator(
        task_id='upload_daily_data',
        python_callable=upload_daily_data,
    )



    upload_daily
