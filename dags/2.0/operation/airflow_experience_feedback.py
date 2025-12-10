from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import gspread
import pandas as pd
from pendulum import timezone

KST = timezone("Asia/Seoul")


feeddback_cycle_1 = '''
-- 1회차 raw
with fdb_o_t as (
select A.lecture_vt_no,A.cycle_count,A.created_at,A.tutor_user_id,date_format(A.created_at,'%Y-%m-%d') as "제출일시",
	cast(max(CASE WHEN A.key in ('"WRdlQ5IHesSBwlRMtm7n"','"PGA4wcjTzXD8gUcA1CEJ"','"uW2XnB5kgOPiF2uNN5bF"','"xOcsN1JRX8Z7eHvc5D9u"') THEN A.value END) as int) AS "수업 추천 점수"
	,cast(max(CASE WHEN A.key in ('"cycle01_01_hello"','"cycle02_01_ready"','"cycle03_01_ready"','"cycle04_01_ready"') THEN A.value END) as int) AS "1.상담메세지발송/수업준비태도"
	,cast(max(CASE WHEN A.key in ('"cycle01_02_promise"','"cycle02_02_question"','"cycle03_02_question"','"cycle04_02_question"') THEN A.value END) as int) AS "2.첫수업가이드/질문하는수업"
	,cast(max(CASE WHEN A.key in ('"cycle01_03_question"','"cycle02_03_compliment"','"cycle03_03_compliment"','"cycle04_03_compliment"') THEN A.value END) as int) AS "3.질문하는수업/칭찬"
	,cast(max(CASE WHEN A.key in ('"cycle01_04_monthlyplan"','"cycle02_04_summary"','"cycle03_04_summary"','"cycle04_04_summary"') THEN A.value END) as int) AS "4.맞춤학습계획서/수업요약"
	,cast(max(CASE WHEN A.key in ('"cycle01_05_respect"','"cycle02_05_respect"','"cycle03_05_respect"','"cycle04_05_respect"') THEN A.value END) as int) AS "5.존중하는태도"
	,cast(max(CASE WHEN A.key in ('"cyMj2Q5EbCE2gOZPuvJs"','"SbXI6fGKqJeuGagtJFqU"','"9qiFkIjQ4ztJE5vyy0rY"','"WwZI08GtA8p7KANNMTuJ"') THEN A.value END) as int) AS "선생님 추천 점수"
	from 		
		(WITH cte AS (
				select row_number() over(partition by sf.lecture_vt_no order by created_at asc) as rn 
					, sf.lecture_vt_no,sf.body_list_map,sf.cycle_count,sf.created_at,sf.tutor_user_id
				FROM marketing_scylladb.marketing_mbti.student_feedback_projects sf
				where sf.cycle_count = 1
				)
				SELECT lecture_vt_no,cycle_count,created_at,tutor_user_id,
					 replace(replace(concat_ws('',map_keys.key),'{',''),'}','') as key,replace(replace(replace(concat_ws('',map_keys.value),'{',''),'}',''),'"','') as value
				FROM cte
				CROSS JOIN UNNEST(split_to_multimap(cte.body_list_map, '",', ':')) map_keys(key, value)
				where rn = 1
		) A		
group by A.lecture_vt_no,A.cycle_count,A.created_at,A.tutor_user_id
),
glvt as (
select glvt.lecture_vt_no, glvt.group_lecture_vt_no, glvt.student_user_no, ttn2.name as grade, ttn.name, glvt.tutoring_state, glvt.done_month, glvt.active_timestamp, glvt.done_timestamp, min_tutoring_datetime, updated_at
	from 
		(select dense_rank() over(partition by lvt.student_user_no order by glvt.active_timestamp asc) as rn
			, glvt.lecture_vt_no, glvt.group_lecture_vt_no, lvt.student_user_no, lvt.lecture_subject_id, lvt.tutoring_state, glvt.done_month, glvt.active_timestamp, glvt.done_timestamp, updated_at, min_tutoring_datetime
			from data_warehouse.raw_data.group_lvt glvt
			inner join mysql.onuei.lecture_video_tutoring lvt on glvt.lecture_vt_no = lvt.lecture_vt_no 
			where glvt.group_lecture_vt_no like '%_1'
		) glvt
	inner join mysql.onuei.student s on glvt.student_user_no = s.user_no 
	inner join mysql.onuei.term_taxonomy_name ttn on glvt.lecture_subject_id = ttn.term_taxonomy_id 
	inner join mysql.onuei.term_taxonomy_name ttn2 on s."year" = ttn2.term_taxonomy_id
	where glvt.rn = 1
	and ttn.name in ('국어','영어','수학')
	and glvt.active_timestamp >= date '2025-11-01'
	and ttn2.name in ('중1','중2','중3','고1','고2')
),
list as (
select glvt.lecture_vt_no, glvt.student_user_no, cast(glvt.name as varchar) as subject, cast(glvt.tutoring_state as varchar) as tutoring_state
	, cast(case when done_timestamp is not null then 'done'
		when done_timestamp is null and tutoring_state in ('DONE','FINISH','AUTO_FINISH') then 'done' 
		when done_timestamp is null and min_tutoring_datetime is null and tutoring_state in ('REGISTER','REMATCH','MATCHED','REMATCH_B') then 'active'
		when done_timestamp is null and min_tutoring_datetime is not null then 'active'
		else 'error' end
		as varchar)
			as active_state
	, glvt.done_month, glvt.active_timestamp, glvt.done_timestamp
	, fdb_o_t.created_at, fdb_o_t."1.상담메세지발송/수업준비태도", fdb_o_t."2.첫수업가이드/질문하는수업", fdb_o_t."3.질문하는수업/칭찬",fdb_o_t."4.맞춤학습계획서/수업요약",fdb_o_t."5.존중하는태도",fdb_o_t."선생님 추천 점수"
	from glvt
	inner join fdb_o_t on (glvt.lecture_vt_no = fdb_o_t.lecture_vt_no and fdb_o_t.created_at between glvt.active_timestamp and glvt.updated_at)
)
select * from list
'''



def upload_backup_table(sql : str, **context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from sqlalchemy import text
    from airflow.providers.trino.hooks.trino import TrinoHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_2.0')  
    pg_engine = pg_hook.get_sqlalchemy_engine()
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    


    print(f'=================={type(sql)}========================')
    print(sql)
    df = pd.read_sql_query(sql=text(sql), con=trino_engine)
    df.to_sql(
        name='feedback_1_raw_2511',
        con=pg_engine,
        schema='data_dimensions',
        if_exists='replace',
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
    dag_id='experience_feedback_1_2511',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # 매주 토요일 오전 6시
    catchup=False,
    tags=['2.0', 'operation', 'experience'],
) as dag:



    upload_backup_active_student = PythonOperator(
        task_id='upload_weekly_active_student_data_backup',
        python_callable=upload_backup_table,
        op_kwargs={
            "sql": feeddback_cycle_1,
        },
        retries=5,
        retry_delay=timedelta(seconds=2),
    )


    upload_backup_active_student
