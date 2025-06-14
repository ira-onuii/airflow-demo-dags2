from datetime import datetime, timedelta

date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

table_name = f'"{date}"'



lvs_query_template = '''
with lvs as (
select lvs.lecture_vt_No, lvs.lecture_cycle_No, lvs.schedule_no, lvs.tutoring_datetime, lvs.schedule_state, sf.student_user_no, sf.teacher_user_No, u.name as teacher_name
	from mysql.onuei.lecture_vt_schedules lvs
	inner join mysql.onuei.student_follow sf on lvs.follow_no = sf.follow_no
    inner join mysql.onuei."user" u on sf.teacher_user_No = u.user_No 
    -- where lvs.create_datetime >= timestamp '2025-03-27 00:00:00'
    -- and lvs.create_datetime < timestamp '2025-04-02 16:50:00'
	where lvs.create_datetime >= timestamp '{{ data_interval_start }}'
    and lvs.create_datetime < timestamp '{{ data_interval_end }}'
)
select * from lvs
'''

lvc_query_template = '''
with lvc as (
select lvc.lecture_cycle_No, lvc.lecture_vt_no, lvc.page_call_room_id
	from mysql.onuei.lecture_vt_cycles lvc
    -- where lvc.req_datetime >= timestamp '2025-03-27 00:00:00'
    -- and lvc.req_datetime < timestamp '2025-04-02 16:50:00'
    where lvc.req_datetime >= timestamp '{{ data_interval_start }}'
	and lvc.req_datetime < timestamp '{{ data_interval_end }}'
)
select * from lvc
'''

mlvt_query = '''
with mlvt as (
select mlvt.lectures[1][1] as lecture_vt_No, mlvt.matchedat
    , mlvt.matchedteacher[1] as teacher_user_No
    from matching_mongodb.matching.matching_lvt mlvt
    where mlvt.status = 'MATCHED'
)
select * from mlvt
'''

p_query = '''
with p as (
select
    p.lecture_vt_No, p.payment_regdate, p.order_id, p.state 
    from mysql.onuei.payment p
)
select * from p
'''

t_query = '''
with t as (
	select t.user_No, t.seoltab_state_updateat, t.lecture_phone_number 
		from mysql.onuei.teacher t
		where t.seoltab_state = 'ACTIVE'
	and t.seoltab_state_updateat >= cast('2024-11-01 00:00:00' as timestamp)
)
select * from t
'''

meta_data_query = '''
with meta_data as (
select lvt.student_user_No ,lvt.lecture_vt_No, ttn.name as subject, th.tteok_ham_type, u.phone_number, lvt.tutoring_state, u.name as student_name
    , lvt.reactive_datetime 
    from mysql.onuei.lecture_video_tutoring lvt
    inner join mysql.onuei."user" u on lvt.student_user_no = u.user_no 
    inner join mysql.onuei.tteok_ham th on lvt.payment_item = th.tteok_ham_no 
    inner join mysql.onuei.term_taxonomy_name ttn on lvt.lecture_subject_id = ttn.term_taxonomy_id 
)
select * from meta_data
'''



