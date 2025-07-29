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
    sheet = google_conn(sheet_name='과외신청서 미작성 CS 확인용의 사본')
    col_range = "B2:E"
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
    sheet = google_conn(sheet_name='과외신청서 미작성 CS 확인용의 사본')
    sheet.update(range_start_cell, dataframe)


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
			inner join lvt on (p.lecture_vt_no = lvt.lecture_vt_no and cast(date_format(p.payment_regdate,'%Y-%m-%d') as timestamp) >= cast(date_format(lvt.crda,'%Y-%m-%d') as timestamp))
		) p
	where p.rn = 1
	and p.state = '결제완료'
	and date_diff('day', p.payment_regdate, now()) >= 3
)
select p.lecture_vt_No, p.student_name, p.student_user_No, p.subject
	from p
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
            lecture_vt_no = str(row[0]).strip()
            key = lecture_vt_no
            existing_keys.add(key)

    # 새로운 DF에서 중복 아닌 것만 필터링
    filtered_df = new_df[
        ~new_df.apply(lambda x: f"{str(x['lecture_vt_No']).strip()}", axis=1).isin(existing_keys)
    ]
    return filtered_df


def upload_daily_data():
    sheet = google_conn(sheet_name='과외신청서 미작성 CS 확인용의 사본')

    # 1. 쿼리 실행
    new_df = run_query()
    print(f'### today_data ### : {len(new_df)}')

    # 2. 기존 데이터 가져오기
    existing_rows = sheet.get("B2:E")
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

def get_empty_f_column_lecture_vt_nos():
    """F열이 비어있는 행들의 lecture_vt_No를 가져오는 함수"""
    sheet = google_conn(sheet_name='과외신청서 미작성 CS 확인용의 사본')
    
    # B열과 F열 데이터 가져오기 (충분한 범위로 설정)
    data_range = "B2:F"  # 적절한 범위로 조정
    data_values = sheet.get(data_range)
    
    empty_i_lecture_vt_nos = []
    
    for i, row in enumerate(data_values, start=2):
        if len(row) >= 1 and row[0]:  # B열에 값이 있는지 확인
            lecture_vt_no = str(row[0]).strip()
            i_column_value = row[4] if len(row) > 4 and row[4] else None  # F열 값
            
            # B열에 값이 있고 F열이 비어있는 경우
            if lecture_vt_no and not i_column_value:
                empty_i_lecture_vt_nos.append(lecture_vt_no)
    
    return empty_i_lecture_vt_nos


def run_application_datetime_query(lecture_vt_no_list):
    """lecture_vt_No 리스트로 application_datetime을 조회하는 함수"""
    if not lecture_vt_no_list:
        return pd.DataFrame()
    
    from airflow.providers.trino.hooks.trino import TrinoHook
    
    # IN 절을 위한 문자열 생성
    lecture_vt_no_str = "','".join(lecture_vt_no_list)
    
    query = f'''
        select lecture_vt_No, application_datetime
        from mysql.onuei.lecture_video_tutoring
        where lecture_vt_No in ('{lecture_vt_no_str}')
    '''
    
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, trino_engine)
    return df


def update_f_column_with_application_datetime():
    """F열에 application_datetime 값을 업데이트하는 함수"""
    sheet = google_conn(sheet_name='과외신청서 미작성 CS 확인용의 사본')
    
    # 1. I열이 비어있는 행들의 lecture_vt_No 가져오기
    empty_f_lecture_vt_nos = get_empty_f_column_lecture_vt_nos()
    print(f'### F열이 비어있는 lecture_vt_No 개수 ### : {len(empty_f_lecture_vt_nos)}')
    
    if not empty_f_lecture_vt_nos:
        print('### F열이 비어있는 행이 없습니다 ###')
        return
    
    # 2. application_datetime 조회
    datetime_df = run_application_datetime_query(empty_f_lecture_vt_nos)
    print(f'### 조회된 application_datetime 개수 ### : {len(datetime_df)}')
    
    if datetime_df.empty:
        print('### 조회된 application_datetime이 없습니다 ###')
        return
    
    # 3. lecture_vt_No별 application_datetime 딕셔너리 생성
    datetime_dict = dict(zip(datetime_df['lecture_vt_No'].astype(str), 
                           datetime_df['application_datetime']))
    
    # 4. 시트의 모든 데이터 가져오기 (B열과 F열)
    data_range = "B2:F"
    data_values = sheet.get(data_range)
    
    # 5. 업데이트할 셀들 찾기 및 업데이트
    update_count = 0
    
    for i, row in enumerate(data_values, start=2):
        if len(row) >= 1 and row[0]:  # B열에 값이 있는지 확인
            lecture_vt_no = str(row[0]).strip()
            f_column_value = row[4] if len(row) > 4 and row[4] else None
            
            # F열이 비어있고 딕셔너리에 해당 lecture_vt_No가 있는 경우
            if not f_column_value and lecture_vt_no in datetime_dict:
                cell_range = f"I{i}"
                cell_value = datetime_dict[lecture_vt_no]
                sheet.update(cell_range, [[cell_value]])
                update_count += 1
    
    # 6. 업데이트 결과 출력
    if update_count > 0:
        print(f'### 업데이트 완료된 셀 개수 ### : {update_count}')
        print('### F열 업데이트 완료 ###')
    else:
        print('### 업데이트할 데이터가 없습니다 ###')

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, tzinfo=KST),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='CS_google_sheet_update_dag_test',
    default_args=default_args,
    schedule_interval='0 10 * * *',  # 매일 오전 10시
    catchup=False,
    tags=['2.0', 'operation', 'CS'],
) as dag:

    upload_daily = PythonOperator(
        task_id='upload_daily_data',
        python_callable=upload_daily_data,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )

    upload_daily_application_datetime = PythonOperator(
        task_id='upload_daily_application_datetime',
        python_callable=update_f_column_with_application_datetime,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )



    upload_daily >> upload_daily_application_datetime
