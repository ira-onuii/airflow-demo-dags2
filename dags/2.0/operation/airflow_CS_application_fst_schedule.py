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
    sheet = client.open_by_key('1fp9UD9kBKtDhFSY-XcTq5ZBF4xIR70FdQ70a09EUBYY').worksheet(sheet_name)
    return sheet

# 쿼리 결과를 시트에 업로드
# def update_google_sheet(range_start_cell, dataframe):
#     sheet = google_conn(sheet_name='과외신청서 미작성 CS 확인용')

    
#     # Pandas DF를 시트에 쓰기 위해 리스트 변환
#     values = [dataframe.columns.tolist()] + dataframe.values.tolist()
#     sheet.update(range_start_cell, values)

#빈 행 탐색
def found_blank():
    sheet = google_conn(sheet_name='첫 수업 미진행')
    col_range = "B2:M"
    col_values = sheet.get(col_range)

    first_empty_row = 2
    for i, row in enumerate(col_values, start=2):
        if not row or not row[0]:  # student_name 기준
            first_empty_row = i
            break
    else:
        first_empty_row = len(col_values) + 2

    return f"B{first_empty_row}"


def update_google_sheet_append_by_column(range_start_cell, dataframe):
    sheet = google_conn(sheet_name='첫 수업 미진행')
    sheet.update(range_start_cell, dataframe)


def run_query():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = '''
with list as (
select glvt.lecture_vt_no, lvs.schedule_no, lvs.schedule_state, lvs.tutoring_datetime
	from mysql.onuei.lecture_vt_schedules lvs
	inner join data_warehouse.raw_data.group_lvt glvt on lvs.schedule_no = glvt.min_schedule_no
	where cast(lvs.tutoring_datetime as date) = cast(now()- interval '1' day as date)
	and schedule_state not in ('TUTORING','DONE')
),
meta as (
select lvt.lecture_vt_no, u.name as student_name, lvt.student_user_no, ttn.name as subject, s.parent_phone_number
	, lvt.tutoring_state, u2.name as teacher_name, ltvt.teacher_user_no, u2.phone_number as teacher_phone_number
	from mysql.onuei.lecture_video_tutoring lvt
	inner join mysql.onuei.lecture_teacher_vt ltvt on lvt.lecture_vt_no = ltvt.lecture_vt_no 
	inner join mysql.onuei."user" u on lvt.student_user_no = u.user_no
	inner join mysql.onuei."user" u2 on lvt.student_user_no = u2.user_no
	inner join mysql.onuei.student s on lvt.student_user_no = s.user_no 
	inner join mysql.onuei.term_taxonomy_name ttn on lvt.lecture_subject_id = ttn.term_taxonomy_id 
	where ltvt.teacher_vt_status = 'ASSIGN'
)
select schedule_no, list.lecture_vt_No, student_name, student_user_No, subject, parent_phone_number, tutoring_state, schedule_state, tutoring_datetime, teacher_name, teacher_phone_number
	from list
	inner join meta on list.lecture_vt_No = meta.lecture_vt_No
    '''
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, trino_engine)
    return df


def filter_duplicates(new_df, existing_rows):
    # 기존 값으로 키 만들기 (user_no + subject)
    existing_keys = set()
    for row in existing_rows:
        if len(row) >= 3:
            schedule_no = str(row[0]).strip()
            key = schedule_no
            existing_keys.add(key)

    # 새로운 DF에서 중복 아닌 것만 필터링
    filtered_df = new_df[
        ~new_df.apply(lambda x: f"{str(x['schedule_no']).strip()}", axis=1).isin(existing_keys)
    ]
    return filtered_df


def upload_daily_data():
    sheet = google_conn(sheet_name='첫 수업 미진행')

    # 1. 쿼리 실행
    new_df = run_query()
    print(f'### today_data ### : {len(new_df)}')

    # 2. 기존 데이터 가져오기
    existing_rows = sheet.get("B2:M")
    print(f'### existing_data ### : {len(existing_rows)}')

    # 3. 중복 제거
    filtered_df = filter_duplicates(new_df, existing_rows)
    print(f'### filtered_data ### : {len(filtered_df)}')

    # 4. 데이터 남아있으면 append
    if not filtered_df.empty:
        update_google_sheet_append_by_column(
            range_start_cell=found_blank(),
            dataframe=filtered_df.values.tolist()
        )


# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, tzinfo=KST),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='CS_fst_schedule_google_sheet_update_dag',
    default_args=default_args,
    schedule_interval='0 9 * * *',  # 매일 오전 10시
    catchup=False,
    tags=['2.0', 'operation', 'CS'],
) as dag:

    upload_daily = PythonOperator(
        task_id='upload_daily_data',
        python_callable=upload_daily_data,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )





    upload_daily
