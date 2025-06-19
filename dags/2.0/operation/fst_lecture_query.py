from datetime import datetime, timedelta
from airflow_incremental_voice_data import max_updated_at

date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

table_name = f'"{date}"'

max_updatedat = max_updated_at()[0]

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



voice_data_query = f'''
with voice as (
select trim(da.room_id) as room_id, da.student_tokens, da.student_lq, da.teacher_tokens 
	from ai_analysis_mongodb.analysis_db."data" da
),
lvc as (
select lvc.lecture_cycle_no, lvc.lecture_vt_no, trim(lvc.page_call_room_id) as room_id, lvc.durations
	, voice.student_tokens, voice.student_lq, voice.teacher_tokens
	from mysql.onuei.lecture_vt_cycles lvc
	inner join voice on trim(lvc.page_call_room_id) = voice.room_id
),
lvs as (
select lvs.follow_no, lvs.schedule_no, lvs.lecture_vt_no, lvs.lecture_cycle_no, lvs.is_free, lvs.schedule_state, lvs.tutoring_datetime, lvs.update_datetime, lvs.per_done_month, lvs.cycle_payment_item  
	from mysql.onuei.lecture_vt_schedules lvs
	where lvs.lecture_cycle_no in (select lecture_cycle_no from lvc)
    and lvs.update_datetime >= {max_updatedat}
),
lvcs as (
select lvs.follow_no, lvs.lecture_vt_no, lvs.schedule_no, lvs.is_free, lvs.schedule_state, lvs.tutoring_datetime, lvs.update_datetime as time_stamp, lvs.per_done_month, lvs.cycle_payment_item
	, lvc.room_id, lvc.durations, lvc.student_tokens, lvc.student_lq, lvc.teacher_tokens, 'active' as active_state
	from lvs 
	inner join lvc on lvs.lecture_cycle_no = lvc.lecture_cycle_no 
),
lcf as (
select null as follow_no, lcf.lecture_vt_no, null as schedule_no, null as is_free, null as schedule_state, null as tutoring_datetime, lcf.update_datetime as time_stamp, null as per_done_month, null as cycle_payment_item
	, null as room_id, null as durations, null as student_tokens, null as student_lq, null as teacher_tokens, 'done' as active_state
	from mysql.onuei.lecture_change_form lcf
	inner join mysql.onuei.student_change_pause scp on lcf.lecture_change_form_no = scp.lecture_change_form_no 
	where lcf.form_type = '중단'
	and lcf.process_status = '안내완료'
	and scp.resume_left_lecture = '중단'
	and lcf.lecture_vt_no in (select lecture_vt_no from lvc)
    and lcf.update_datetime >= {max_updatedat}
	-- and lcf.update_datetime >= cast('2024-11-01' as timestamp)
	-- and lcf.lecture_vt_no = 42223
),
list as (
select *
	from lvcs
union all
select *
	from lcf
	order by time_stamp asc
)
-- select * from list
,
list_2 as (
select sum(case when active_state = 'done' then 1 else 0 end) over(partition by lecture_vt_no order by tutoring_datetime nulls last, time_stamp asc) + 1 as raw_group_no
	,*
	from list
),
list_3 as (
select case when active_state = 'done' then raw_group_no-1 else raw_group_no end as seq, *
	from list_2
),
list_4 as (
select lecture_vt_no, concat(cast(lecture_vt_No as varchar),'_', cast(seq as varchar)) as group_lecture_vt_No,time_stamp, schedule_No, active_state
	, room_id, tutoring_datetime
	, schedule_state, per_done_month 
	, sum(case when per_done_month is null then 0 else per_done_month end) over(partition by concat(cast(lecture_vt_No as varchar),'_', cast(seq as varchar)) order by tutoring_datetime nulls last,time_stamp) as sum_done_month
	, student_tokens, teacher_tokens, student_lq 
	from list_3
)
select * from list_4
'''

