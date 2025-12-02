from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pendulum import timezone

KST = timezone("Asia/Seoul")


active_student_query = '''
    with list as (
select lvt.student_user_no, u.phone_number, ttn.name as grade
	from mysql.onuei.lecture_video_tutoring lvt 
	inner join mysql.onuei."user" u on lvt.student_user_No = u.user_No 
	inner join mysql.onuei.student s on student_user_No = s.user_No 
	inner join mysql.onuei.term_taxonomy_name ttn on s."year" = ttn.term_taxonomy_id 
	where student_type in ('PAYED','PAYED_B')
	and tutoring_state not in ('FINISH','AUTO_FINISH','DONE')
	and u.email_id not like '%test%'
	and ttn.name not in ('N수생','초1','초2','초3','초4','초5','초6')
	group by lvt.student_user_No, u.phone_number, ttn.name 
)
select student_user_no, phone_number, grade, now() as updated_at
    from list
'''

inative_student_query = '''
-- 중단_학생
with list as (
select lvt.student_user_no, u.phone_number, ttn.name as grade, max(lvt.update_datetime) as max_done_time
	from mysql.onuei.lecture_video_tutoring lvt 
	inner join mysql.onuei."user" u on lvt.student_user_No = u.user_No 
	inner join mysql.onuei.student s on student_user_No = s.user_No 
	inner join mysql.onuei.term_taxonomy_name ttn on s."year" = ttn.term_taxonomy_id 
	where student_type in ('PAYED','PAYED_B')
	and tutoring_state in ('FINISH','AUTO_FINISH','DONE')
	and u.email_id not like '%test%'
	and ttn.name not in ('N수생','초1','초2','초3','초4','초5','초6')
	group by lvt.student_user_No, u.phone_number, ttn.name 
)
select list.student_user_no, list.phone_number, list.grade, now() as updated_at
	from list
	where list.student_user_no not in (
	select student_user_No 
		from mysql.onuei.lecture_video_tutoring lvt 
		where tutoring_state not in ('FINISH','AUTO_FINISH','DONE')
	)
	and max_done_time <= '2024-08-01 23:59:59'
'''

active_parent_query = '''
    with list as (
select lvt.student_user_no, s.parent_phone_number, ttn.name as grade
	from mysql.onuei.lecture_video_tutoring lvt 
	inner join mysql.onuei."user" u on lvt.student_user_No = u.user_No 
	inner join mysql.onuei.student s on student_user_No = s.user_No 
	inner join mysql.onuei.term_taxonomy_name ttn on s."year" = ttn.term_taxonomy_id 
	where student_type in ('PAYED','PAYED_B')
	and tutoring_state not in ('FINISH','AUTO_FINISH','DONE')
	and u.email_id not like '%test%'
	and ttn.name not in ('N수생','초1','초2','초3','초4','초5','초6')
	group by lvt.student_user_No, u.phone_number, ttn.name 
)
select student_user_no, parent_phone_number, grade, now() as updated_at
    from list
'''

inactive_parent_query = '''-- 중단_학부모
with list as (
select lvt.student_user_no, s.parent_phone_number, ttn.name as grade, max(lvt.update_datetime) as max_done_time
	from mysql.onuei.lecture_video_tutoring lvt 
	inner join mysql.onuei."user" u on lvt.student_user_No = u.user_No 
	inner join mysql.onuei.student s on student_user_No = s.user_No 
	inner join mysql.onuei.term_taxonomy_name ttn on s."year" = ttn.term_taxonomy_id 
	where student_type in ('PAYED','PAYED_B')
	and tutoring_state in ('FINISH','AUTO_FINISH','DONE')
	and u.email_id not like '%test%'
	and ttn.name not in ('N수생','초1','초2','초3','초4','초5','초6')
	group by lvt.student_user_No, u.phone_number, ttn.name 
)
select list.student_user_no, list.parent_phone_number, list.grade, now() as updated_at
	from list
	where list.student_user_no not in (
	select student_user_No 
		from mysql.onuei.lecture_video_tutoring lvt 
		where tutoring_state not in ('FINISH','AUTO_FINISH','DONE')
	)
	and max_done_time <= '2024-08-01 23:59:59'
'''

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
    sheet = client.open_by_key('1lRnwDtPm8vv6fmBpES5EmKw_LdbivNUQAqfo-JkRkSM').worksheet(sheet_name)
    return sheet


def clear_sheet(sheet_name):
    try:
        # google_conn 함수를 호출하여 연결 객체를 얻습니다.
        connection = google_conn(sheet_name)
        connection.batch_clear(["A2:C"])
    except gspread.exceptions.WorksheetNotFound as e:
        print(e)
    # 시트가 존재하지 않는 경우
        pass
    return clear_sheet



def update_google_sheet_active_student(dataframe):
    df = dataframe.copy()
    df = df.where(df.notnull(), "").astype(str) 
    sheet = google_conn(sheet_name='학생_수강생')
    sheet.batch_clear(["A2:D"])
    sheet.update("A2:D", df.values.tolist())

def update_google_sheet_inactive_student(dataframe):
    df = dataframe.copy()
    df = df.where(df.notnull(), "").astype(str) 
    sheet = google_conn(sheet_name='학생_중단')
    sheet.batch_clear(["A2:D"])
    sheet.update("A2:D", df.values.tolist())

def update_google_sheet_active_parent(dataframe):
    df = dataframe.copy()
    df = df.where(df.notnull(), "").astype(str) 
    sheet = google_conn(sheet_name='학부모_수강생')
    sheet.batch_clear(["A2:D"])
    sheet.update("A2:D", df.values.tolist())

def update_google_sheet_inactive_parent(dataframe):
    df = dataframe.copy()
    df = df.where(df.notnull(), "").astype(str) 
    sheet = google_conn(sheet_name='학부모_중단')
    sheet.batch_clear(["A2:D"])
    sheet.update("A2:D", df.values.tolist())


def run_query_active_student():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = active_student_query
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, trino_engine)
    return df


def run_query_inactive_student():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = incative_student_query
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, trino_engine)
    return df

def run_query_active_parent():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = active_parent_query
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, trino_engine)
    return df


def run_query_inactive_parent():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = inactive_parent
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, trino_engine)
    return df


def active_student_listup():
    update_google_sheet_active_student(dataframe=run_query_active_student())

def inactive_student_listup():
    update_google_sheet_active_student(dataframe=run_query_inactive_student())

def active_parent_listup():
    update_google_sheet_active_student(dataframe=run_query_active_parent())

def inactive_parent_listup():
    update_google_sheet_active_student(dataframe=run_query_inactive_parent())


def upload_backup_table(sql : str):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from sqlalchemy import text
    pg_hook = PostgresHook(postgres_conn_id='postgres_marketing')  
    pg_engine = pg_hook.get_sqlalchemy_engine()
    
    print(f'=================={type(sql)}========================')
    print(sql)
    df = pd.read_sql_query(sql=text(sql), con=pg_engine)
    print(df)
    df.to_sql(
        name='seoltab_letter_backup',
        con=pg_engine,
        schema='public',
        if_exists='append',
        index=False
    )


# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, tzinfo=KST),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='marketing_seoltab_letter_google_sheet_update_dag',
    default_args=default_args,
    schedule_interval='0 10 * * *',  # 매일 오전 10시
    catchup=False,
    tags=['2.0', 'operation', 'marketing'],
) as dag:

    upload_active_student = PythonOperator(
        task_id='upload_weekly_active_student_data',
        python_callable=active_student_listup,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )

    upload_inactive_student = PythonOperator(
        task_id='upload_weekly_inactive_student_data',
        python_callable=inactive_student_listup,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )
    upload_active_parent = PythonOperator(
        task_id='upload_weekly_active_parent_data',
        python_callable=active_parent_listup,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )
    upload_inactive_parent = PythonOperator(
        task_id='upload_weekly_inactive_parent_data',
        python_callable=inactive_parent_listup,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )


    upload_backup_active_student = PythonOperator(
        task_id='upload_weekly_active_student_data_backup',
        python_callable=upload_backup_table,
        op_kwargs={
            "sql": active_student_query
        },
        retries=5,
        retry_delay=timedelta(seconds=2),
    )

    upload_backup_inactive_student = PythonOperator(
        task_id='upload_weekly_inactive_student_data_backup',
        python_callable=upload_backup_table,
        op_kwargs={
            "sql": inative_student_query
        },
        retries=5,
        retry_delay=timedelta(seconds=2),
    )

    upload_backup_active_parent = PythonOperator(
        task_id='upload_weekly_active_parent_data_backup',
        python_callable=upload_backup_table,
        retries=5,
        op_kwargs={
            "sql": active_parent_query
        },
        retry_delay=timedelta(seconds=2),
    )

    upload_backup_inactive_parent = PythonOperator(
        task_id='upload_weekly_inactive_parent_data_backup',
        python_callable=upload_backup_table,
        op_kwargs={
            "sql": inactive_parent_query
        },
        retries=5,
        retry_delay=timedelta(seconds=2),
    )

    
    



    upload_active_student >> upload_backup_active_student >> upload_inactive_student >> upload_backup_inactive_student >> upload_active_parent >> upload_backup_active_parent >> upload_inactive_parent >> upload_backup_inactive_parent
