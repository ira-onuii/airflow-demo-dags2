import sys
import os

# 현재 파일이 있는 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import fst_lecture_query

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

user_filename = 'list_test.csv'



   
def fst_lecture_save_to_s3_with_hook(data, bucket_name, file_name):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)

    hook = S3Hook(aws_conn_id='conn_S3')
    hook.load_string(csv_buffer.getvalue(), key=f"{file_name}", bucket_name=bucket_name, replace=True)




def fst_lecture_save_results_to_s3(**context):
    column_names = ["lecture_vt_No", "subject", "student_user_No", "student_name","teacher_user_No","teacher_name","rn","page_call_room_id","tutoring_datetime"]
    try:
        existing_df = pd.read_csv("s3://seoltab-datasource/list_test.csv")
    except FileNotFoundError:
        print("파일이 존재하지 않습니다. 새로 생성합니다.")
        existing_df = pd.DataFrame()
    query_results = context['ti'].xcom_pull(task_ids='fst_lecture_run_select_query_test')
    query_results = pd.DataFrame(query_results, columns=column_names)
    updated_df = pd.concat([existing_df, query_results], ignore_index=True)
    updated_df = updated_df.drop_duplicates(subset=['page_call_room_id'], keep='last')
    fst_lecture_save_to_s3_with_hook(updated_df, 'seoltab-datasource', user_filename)



default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'data-warehouse-test-tirno-AI_2.0_test',
    default_args=default_args,
    description='Run query and load result to S3',
    start_date=datetime(2024, 11, 13, 6, 25),
    tags=["2.0"],
    schedule='*/25 * * * *',
    catchup=False
)



#user
fst_lecture_run_query = SQLExecuteQueryOperator(
    task_id='fst_lecture_run_select_query_test',
    sql=f'''
with lvc as (
select row_number() over(partition by lvs.lecture_vt_no order by lvs.tutoring_datetime asc) as rn
	, lvs.lecture_vt_no, sf.teacher_user_no, lvc.page_call_room_id, lvs.tutoring_datetime 
	from mysql.onuei.lecture_vt_schedules lvs
	inner join mysql.onuei.student_follow sf on lvs.follow_no = sf.follow_no 
	inner join mysql.onuei.lecture_vt_cycles lvc on lvs.lecture_cycle_no = lvc.lecture_cycle_no 
	where lvc.req_datetime >= '{{ data_interval_start }}'
	and lvc.req_datetime >= '{{ data_interval_end }}'
    and lvs.tutoring_datetime >= '{{ data_interval_start }}'
    and lvs.tutoring_datetime >= '{{ data_interval_end }}'
),
mlvt as (
	select mlvt.lecture_vt_No
		, mlvt.teacher_user_No, mlvt.teacher_name, mlvt.matchedat, student_name
		from 
			(select row_number() over(partition by lectures[1][1] order by mlvt.matchedat asc) as rn, matchedat
				, mlvt.lectures[1][1] as lecture_vt_No
				, mlvt.matchedteacher[1] as teacher_user_No, mlvt.matchedteacher[2] as teacher_name, mlvt.lectures[1][2][2] as student_name
				from matching_mongodb.matching.matching_lvt mlvt
				where mlvt.status = 'MATCHED'
			) mlvt
		where mlvt.rn = 1
),
	p as (
		select p.lecture_vt_no, p.payment_regdate, p.order_id, p.state 
			from 
				(select row_number() over(partition by p.lecture_vt_no order by p.payment_regdate asc) as rn 
					, p.lecture_vt_no, p.payment_regdate, p.order_id, p.state 
					from mysql.onuei.payment p
				) p
			where p.rn = 1
			and p.payment_regdate >= cast('2024-11-01 00:00:00' as timestamp)
),
	t as (
		select t.user_No, t.seoltab_state_updateat, t.lecture_phone_number 
			from mysql.onuei.teacher t
			where t.seoltab_state = 'ACTIVE'
			and t.seoltab_state_updateat >= cast('2024-11-01 00:00:00' as timestamp)
),
	meta_data as (
		select lvt.student_user_No ,lvt.lecture_vt_no, ttn.name as subject, th.tteok_ham_type, u.phone_number, lvt.tutoring_state, u.name as student_name
			, lvt.reactive_datetime 
			from mysql.onuei.lecture_video_tutoring lvt
			inner join mysql.onuei."user" u on lvt.student_user_no = u.user_no 
			inner join mysql.onuei.tteok_ham th on lvt.payment_item = th.tteok_ham_no 
			inner join mysql.onuei.term_taxonomy_name ttn on lvt.lecture_subject_id = ttn.term_taxonomy_id 
			-- where lvt.reactive_datetime is null
			-- and lvt.tutoring_state = 'ACTIVE'
)
select mlvt.lecture_vt_No, md.subject, md.student_user_No, mlvt.student_name
	, mlvt.teacher_user_No, mlvt.teacher_name, lvc.rn, lvc.page_call_room_id, lvc.tutoring_datetime
	from mlvt
	inner join lvc on (mlvt.lecture_vt_No = lvc.lecture_vt_no and mlvt.teacher_user_No = lvc.teacher_user_No)
	inner join p on mlvt.lecture_vt_No = p.lecture_vt_no 
	left join meta_data md on md.lecture_vt_No = mlvt.lecture_vt_No
	inner join t on mlvt.teacher_user_No = t.user_no 
	order by mlvt.lecture_vt_No, lvc.rn
''',
    conn_id='trino_conn',
    do_xcom_push=True,
    dag=dag,
)


fst_lecture_save_to_s3_task = PythonOperator(
    task_id='fst_lecture_list_save_to_s3_test',
    python_callable=fst_lecture_save_results_to_s3,
    provide_context=True,
)






# dbt_run = BashOperator(
#     task_id='dbt_run',
#     bash_command='dbt run --profiles-dir /opt/airflow/dbt_project/.dbt --project-dir /opt/airflow/dbt_project --models /opt/airflow/dbt_project/models/pg_active_lecture/active_lecture.sql',
# )

fst_lecture_run_query >> fst_lecture_save_to_s3_task 


