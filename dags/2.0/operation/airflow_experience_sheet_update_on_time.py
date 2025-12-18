from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pendulum import timezone

KST = timezone("Asia/Seoul")


list_query = '''
with 
    glvt as (
            select glvt.group_lecture_vt_no,glvt.lecture_vt_no , glvt.active_timestamp , glvt.done_month ,glvt.done_timestamp,
                    LEAD(active_timestamp) OVER (PARTITION BY lecture_vt_no ORDER BY active_timestamp) AS next_active_timestamp
                    from data_warehouse.raw_data.group_lvt glvt
                    where glvt.active_timestamp >= timestamp '2025-11-01 00:00:00'
                ),
    lvts as (
            select lvt.lecture_vt_no ,lvt.student_type, u.user_no, u.name,u.phone_number,u.email_id,s.parent_name ,s.parent_phone_number ,lvt.payment_item , lvt.application_datetime, lvt.tutoring_state, ttn.name as "subject",lvt.lecture_subject_id, lvt.total_subject_done_month
            from mysql.onuei.lecture_video_tutoring lvt
            inner join 
                    (select u.user_no, u.name, u.phone_number, u.email_id
                            from mysql.onuei.user u) u on lvt.student_user_no = u.user_no
            left join mysql.onuei.student s on lvt.student_user_no = s.user_no 
            left join mysql.onuei.term_taxonomy_name ttn on lvt.lecture_subject_id = ttn.term_taxonomy_id 
            where lvt.student_type not in ('CTEST')
          ),
    t as (
            select t.user_no ,t.seoltab_state ,t.seoltab_state_updateat ,ut.name as t_name, t.lecture_phone_number 
                    from mysql.onuei.teacher t
                    left join mysql.onuei.user ut on t.user_no = ut.user_no 
                    where t.seoltab_state is not null
                    and t.lecture_phone_number not in ('01000000000')
                    and t.lecture_phone_number not in ('01099999999')
            ),
         sch AS (
            SELECT *
            FROM (
                select
                    glvt.group_lecture_vt_no,
                    lvs.lecture_vt_no,
                    lvs.schedule_no,
                    lvs.create_datetime,
                    lvs.tutoring_datetime,
                    lvs.update_datetime,
                    lvs.schedule_state,
                    sf.teacher_user_no,
                    sf.follow_no,
                    lvs.lecture_cycle_no,
                    sf.student_user_no,
                    -- 🔧 스케줄 순서 번호 추가
                    ROW_NUMBER() OVER (PARTITION BY lvs.lecture_vt_no ORDER BY lvs.create_datetime ASC) AS schedule_rank,
                    -- 🔧 누적 DONE 카운트
                    SUM(CASE WHEN lvs.schedule_state = 'DONE' THEN 1 ELSE 0 END)
                        OVER (PARTITION BY lvs.lecture_vt_no ORDER BY lvs.create_datetime ASC) AS done_rank
                FROM glvt
                left join mysql.onuei.lecture_vt_schedules lvs on glvt.lecture_vt_no = lvs.lecture_vt_no 
                    and glvt.active_timestamp <= lvs.create_datetime 
                    AND (glvt.next_active_timestamp IS NULL OR lvs.create_datetime < glvt.next_active_timestamp)
                INNER JOIN mysql.onuei.student_follow sf ON lvs.follow_no = sf.follow_no
            ) sub
            WHERE schedule_rank <= 4  -- 🔧 4회차까지 허용
        ),
        feedback as (
                        select concat(cast(lsf.lecture_vt_no as varchar),'_',cast(lsf.teacher_id as varchar),'_',cast(lsf.feedback_cycle as varchar))as key_no, lsf.lecture_vt_no, lsf.student_id , lsf.teacher_id ,lsf.feedback_cycle , lsf.schedule_no , lsf.created_at 
                                from mysql.onuei.lecture_student_feedback lsf 
                                where lsf.created_at >= timestamp '2025-11-01 00:00:00'
                                and feedback_cycle <= 3
                        ),
        nps as (
                select concat(cast(A.lecture_vt_no as varchar),'_',cast(A.tutor_user_id as varchar),'_',cast(A.cycle_count as varchar))as key_no,
                        A.rn, A.lecture_vt_no,A.cycle_count,A.tutor_user_id,date_format(A.created_at + interval '9' hour, '%Y-%m-%d %H:%i:%s') as "제출일시",A.student_user_id
                        ,max(CASE WHEN A.key in ('"cycle01_01_hello"','"cycle02_01_ready"','"cycle03_01_ready"') THEN A.value END) AS "1번"
                        ,max(CASE WHEN A.key in ('"cycle01_02_promise"','"cycle02_02_question"','"cycle03_02_question"') THEN A.value END) AS "2번"
                        ,max(CASE WHEN A.key in ('"cycle01_03_question"','"cycle02_03_compliment"','"cycle03_03_compliment"') THEN A.value END) AS "3번"
                        ,max(CASE WHEN A.key in ('"cycle01_04_monthlyplan"','"cycle02_04_summary"','"cycle03_04_summary"') THEN A.value END) AS "4번"
                        ,max(CASE WHEN A.key in ('"cycle01_05_respect"','"cycle02_05_respect"','"cycle03_05_respect"') THEN A.value END) AS "5번"
                        ,max(CASE WHEN A.key in ('"cyMj2Q5EbCE2gOZPuvJs"','"SbXI6fGKqJeuGagtJFqU"','"9qiFkIjQ4ztJE5vyy0rY"') THEN A.value END) AS "선생님 추천 점수"
                        ,max(CASE WHEN A.key in ('"V3m8LmkMKv265jK0gqGR"','"CO6txyeWj4VL8EQIrkgd"','"WuBLJuA2fRJPHxfp0W3i"') THEN A.value END) AS "중립_개선점"
                        ,max(CASE WHEN A.key in ('"cycle01_under_8_change_tutor"','"cycle02_under_8_change_tutor"','"cycle03_under_8_change_tutor"') THEN A.value END) AS "중립_선생님 변경 희망 여부"
                        ,max(CASE WHEN A.key in ('"y6SQ8lSRfrrhzusiamMo"','"dKuaT9i0aX76oV3wbr6C"','"30y5Fl6sTpwI38EVY41g"') THEN A.value END) AS "비추천_개선점"
                        ,max(CASE WHEN A.key in ('"cycle01_under_6_change_tutor"','"cycle02_under_6_change_tutor"','"cycle03_under_6_change_tutor"') THEN A.value END) AS "비추천_선생님 변경 희망 여부"
                        from (
                                select rn,lecture_vt_no,cycle_count,created_at,tutor_user_id,student_user_id 
                                                ,replace(replace(concat_ws('',map_keys.key),'{',''),'}','') as key,replace(replace(replace(concat_ws('',map_keys.value),'{',''),'}',''),'"','') as value
                                        from (
                                            select *
                                                    from
                                                    (select row_number() over(partition by sf.lecture_vt_no order by sf.created_at asc) as rn,sf.*
                                                            from marketing_scylladb.marketing_mbti.student_feedback_projects sf
                                                            where sf.created_at > timestamp '2025-11-01 00:00:00') sf
                                                    where sf.rn <= 3
                                                    ) nps_raw
                                    CROSS JOIN UNNEST(split_to_multimap(nps_raw.body_list_map, '",', ':')) map_keys(key, value)
                                ) A
                group by A.rn,A.lecture_vt_no,A.cycle_count,A.created_at,A.tutor_user_id,student_user_id 
                ),
            link as ( 
                select sch.schedule_no ,sch.lecture_vt_no, sch.schedule_state, sch.tutoring_datetime , sch.student_user_no,u.name as sname, sch.teacher_user_no,ut.name as tname, lvc.page_call_room_id , lt.pagecall_access_token, lvc.durations
                                ,concat(
                                    'https://app.pagecall.net/replay/',
                                    lvc.page_call_room_id,
                                    '?access_token=',
                                    trim(cast(lt.pagecall_access_token as varchar)),
                                    '&debug=1'
                                ) as link 
                        from sch
                    left join (
                                                  select lecture_cycle_no, 
                                                         rtrim(cast(page_call_room_id as varchar)) as page_call_room_id,
                                                         durations
                                                  from mysql.onuei.lecture_vt_cycles
                                                ) lvc on sch.lecture_cycle_no = lvc.lecture_cycle_no
                        left join mysql.onuei.lecture_tutor lt on lt.user_no = sch.teacher_user_no
                        left join mysql.onuei.user u on sch.student_user_no = u.user_no 
                        left join mysql.onuei.user ut on sch.teacher_user_no  = ut.user_no 
                ),
    matchingdata as (
            select glvt.group_lecture_vt_no, glvt.active_timestamp, glvt.lecture_vt_no
                                    ,row_number () over (partition by glvt.group_lecture_vt_no, md.tutor_id order by md.matchedat asc) as rn, md.*, t.t_name
                    from glvt                
                        left join
                        (SELECT
                          mlvt.lectures[1].id AS lecture_id,
                          mlvt.lectures[1].student.id AS student_id,
                        mlvt.status,mlvt.teachersuggestionstatus,
                        date_add('hour', 9, mlvt.matchedat) as matchedat,
                        mlvt.updatedat,
                        mlvt.matchedteacher.id as tutor_id
                                 from matching_mongodb.matching.matching_lvt mlvt
                                 where DATE_ADD('hour', 9, mlvt.matchedat) > cast ('2025-11-01 00:00:00' as timestamp)
                            )md on glvt.lecture_vt_no = md.lecture_id and glvt.active_timestamp < md.matchedat
                        left join t on md.tutor_id = t.user_no
                        ),
        ltvt as (
                        select glvt.group_lecture_vt_no , glvt.lecture_vt_no , glvt.active_timestamp ,ltvt.teacher_user_no , ltvt.teacher_vt_status , ltvt.total_done_month, ltvt.create_at , ltvt.reactive_at 
                                from glvt
                                left join mysql.onuei.lecture_teacher_vt ltvt on glvt.lecture_vt_no = ltvt.lecture_vt_no AND (glvt.next_active_timestamp IS NULL OR ltvt.reactive_at < glvt.next_active_timestamp)
                        ),
    ticket as (
                    select * from (
                        select 
                            cst.lecture_vt_no, 
                            cst.content, 
                            cst.update_datetime, 
                            substr(cst.content, 34, 3) as tname,
                            row_number() over (partition by cst.lecture_vt_no, substr(cst.content, 34, 3) 
                                              order by cst.update_datetime desc) as rn
                        from mysql.onuei.customer_service_ticket cst 
                        where cst.update_datetime > timestamp '2025-11-01 00:00:00'
                          and cst.content like ('%첫 메시지 도착%')
                    ) sub
                    where rn = 1
                    )
select glvt.group_lecture_vt_no ,
                sch.create_datetime ,
                       fb.key_no,
                sch.lecture_vt_no,
                sch.schedule_state , 
                case
                    when sch.done_rank = 0 and sch.schedule_state in ('RESERVATION','CONTRACT') then '첫 수업 전'
                    when sch.done_rank = 1 and sch.schedule_state in ('DONE') then '1회차 완료'
                    when sch.done_rank = 1 and sch.schedule_state in ('RESERVATION') then '2회차 전'
                    when sch.done_rank = 2 and sch.schedule_state in ('DONE') then '2회차 완료'
                    when sch.done_rank = 2 and sch.schedule_state in ('RESERVATION') then '3회차 전'
                    when sch.done_rank = 3 and sch.schedule_state in ('DONE') then '3회차 완료'
                    -- 🔧 3회차 완료 후 4회차가 대기/예약 상태
                    when sch.done_rank = 3 and sch.schedule_state not in ('DONE') then '종료'
                    -- 🔧 4회차 완료된 경우에도 '종료'로 표시
                    when sch.done_rank >= 4 then '종료'
                    when sch.schedule_state in ('TUTORING') then '수업 중'
                    when sch.schedule_state in ('CANCEL') then '취소'
                    else ' '
                end
                        as "회차",
                glvt.active_timestamp,
                glvt.user_no , 
                glvt.name , 
                glvt.phone_number , 
                glvt.parent_name , 
                glvt.parent_phone_number , 
                t.user_no ,
                t.t_name, 
                t.lecture_phone_number , 
                sch.tutoring_datetime ,
                case 
                        when sch.schedule_state in ('DONE') then sch.update_datetime 
                        else null
                end as "마치기 시점",
                nps."제출일시",
                nps."선생님 추천 점수" ,
                nps."1번" ,
                nps."2번" ,
                nps."3번" ,
                nps."4번" ,
                nps."5번",
                case
                        when nps."중립_개선점" is not null then nps."중립_개선점"
                        when nps."비추천_개선점" is not null then nps."비추천_개선점"
                        else '없음'
                end as "개선점",
                case
                        when nps."중립_선생님 변경 희망 여부" in ('yes') then nps."중립_선생님 변경 희망 여부"
                        when nps."비추천_선생님 변경 희망 여부" in ('yes') then nps."비추천_선생님 변경 희망 여부"
                        else 'X'
                end as "변경희망여부",
                link.link,
                glvt.tutoring_state,
                glvt.lecture_subject_id ,
                glvt.subject,
                md.matchedat,
                ltvt.total_done_month ,
                link.durations ,
                CASE 
                    WHEN date_diff('hour', md.matchedat, ticket.update_datetime) >= 24 THEN '초과'
                    WHEN date_diff('hour', md.matchedat, ticket.update_datetime) < 24 THEN '통과'
                    when sch.schedule_state not in ('DONE') or sch.schedule_state is null then ''
                    ELSE '확인 필요'
                        END AS "위반"
        from (select glvt.group_lecture_vt_no , glvt.active_timestamp , glvt.done_month , lvts.*
                            from glvt
                        left join lvts on glvt.lecture_vt_no = lvts.lecture_vt_no
                        where lvts.name not like ('%테스트%')
                                and lvts.phone_number not like ('%00000000%')
                                and lvts.email_id not like ('%@seoltab.test%')) glvt
        left join sch on glvt.group_lecture_vt_no = sch.group_lecture_vt_no 
        left join t on sch.teacher_user_no = t.user_no 
        left join feedback fb on sch.schedule_no = fb.schedule_no 
        left join nps on nps.key_no = fb.key_no  
        left join link on link.schedule_no = sch.schedule_no
        left join matchingdata md on glvt.group_lecture_vt_no = md.group_lecture_vt_no and md.tutor_id = sch.teacher_user_no and md.rn = 1
        left join ltvt on ltvt.group_lecture_vt_no = glvt.group_lecture_vt_no and ltvt.teacher_user_no = t.user_no
        left join ticket on glvt.lecture_vt_no = ticket.lecture_vt_no and ticket.tname = t.t_name
        order by sch.create_datetime , glvt.lecture_vt_no asc
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
    sheet = client.open_by_key('1htuBC0kD-o1B8_JjYW81fEJ6WDr7_Ox-2mVJtQGsePQ').worksheet(sheet_name) 
    #테스트 : 1htuBC0kD-o1B8_JjYW81fEJ6WDr7_Ox-2mVJtQGsePQ 
    #라이브 : 1JN1V9SFmtIASDplAERz3oDtRhgZgAauqtjg9TbSVHg0
    return sheet





def update_google_sheet_query_result(dataframe):
    df = dataframe.copy()
     # Timestamp/Datetime 컬럼만 골라서 처리
    dt_cols = df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns

    for c in dt_cols:
        # NaT -> "" 처리 후 문자열로
        df[c] = df[c].where(df[c].notna(), "").astype(str)
        # 혹시 남는 "NaT" 방어(환경 따라 생길 수 있어서)
        df[c] = df[c].replace("NaT", "")
    sheet = google_conn(sheet_name='시트161')
    sheet.batch_clear(["B6:AI"])
    sheet.update("B6:AI", df.values.tolist())




def run_query():
    from airflow.providers.trino.hooks.trino import TrinoHook
    query = list_query
    trino_hook = TrinoHook(trino_conn_id='trino_conn')
    trino_engine = trino_hook.get_sqlalchemy_engine()
    df = pd.read_sql(query, trino_engine)
    return df





def update_query_result():
    update_google_sheet_query_result(dataframe=run_query())




# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, tzinfo=KST),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='experience_query_google_sheet_update_dag_on_time',
    default_args=default_args,
    schedule_interval='0 9,12,14,17 * * *',
    catchup=False,
    tags=['2.0', 'operation', 'experience'],
) as dag:

    upload_query_result = PythonOperator(
        task_id='upload_weekly_active_student_data',
        python_callable=update_query_result,
        retries=5,
        retry_delay=timedelta(seconds=2),
    )

 

    
    
    
    upload_query_result 
