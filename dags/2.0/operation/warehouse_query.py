from datetime import datetime, timedelta

date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

from airflow_incremental_group_lvt import max_updated_at

max_updatedat = max_updated_at()[0]

table_name = f'"{date}"'


group_lvt_query = f'''
-- group_lvt
with p as (
select lecture_vt_no, payment_no, payment_regdate
	from mysql.onuei.payment p
	where state = '결제완료'
	-- and payment_regdate >= cast('2024-05-01' as timestamp)
),
lvs as (
select lvs.follow_no, lvs.schedule_no, lvs.lecture_vt_no, lvs.lecture_cycle_no, lvs.is_free, lvs.schedule_state, lvs.tutoring_datetime, lvs.update_datetime, lvs.per_done_month, lvs.cycle_payment_item  
	from mysql.onuei.lecture_vt_schedules lvs
	-- where lvs.schedule_state <> 'CANCEL'
),
lvcs as (
select p.lecture_vt_no, p.payment_no, p.payment_regdate as time_stamp
	, 'active' as active_state
	from p
),
lcf as (
select lcf.lecture_vt_no, null as payment_no, lcf.update_datetime as time_stamp
	,'done' as active_state
	from mysql.onuei.lecture_change_form lcf
	inner join mysql.onuei.student_change_pause scp on lcf.lecture_change_form_no = scp.lecture_change_form_no 
	where lcf.form_type = '중단'
	and lcf.process_status = '안내완료'
	and scp.resume_left_lecture = '중단'
	-- and lcf.create_datetime >= cast('2024-05-01' as timestamp)
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
select sum(case when active_state = 'done' then 1 else 0 end) over(partition by lecture_vt_no order by time_stamp asc) + 1 as raw_group_no
	,*
	from list
	-- where list.lecture_vt_no not in (select lecture_vt_no from list where time_stamp <= cast('2024-01-01 00:00:00' as timestamp))
),
list_3 as (
select case when active_state = 'done' then raw_group_no-1 else raw_group_no end as seq, *
	from list_2
),
list_4 as (
select lecture_vt_no, concat(cast(lecture_vt_no as varchar),'_', cast(seq as varchar)) as group_lecture_vt_no
	,min(case when active_state = 'active' then time_stamp else null end) as active_timestamp
	,max(case when active_state = 'done' then time_stamp else null end) as done_timestamp
	,min(payment_no) as min_payment_no, max(time_stamp) as updated_at
	-- , min(time_stamp) as min_active_done_time_stamp, max(time_stamp) as max_active_done_time_stamp, min(payment_no) as min_payment_no, active_state
	-- , min(time_sta) as min_payment_regdate
	-- , sum(per_done_month) as done_month
	-- , time_stamp, schedule_no, active_state, tutoring_datetime, per_done_month
	from list_3
	group by lecture_vt_no, concat(cast(lecture_vt_no as varchar),'_', cast(seq as varchar))
)
-- select * from list_4 where  active_timestamp >= cast('2024-05-01' as timestamp)
,
list_5 as (
select list_4.lecture_vt_no, list_4.group_lecture_vt_no, list_4.active_timestamp, list_4.done_timestamp
	, cast(case when max(lvs.update_datetime) >= updated_at then max(lvs.update_datetime) else updated_at end as timestamp) as updated_at 
	, min(tutoring_datetime) as min_tutoring_datetime, min(schedule_no) as min_schedule_no, sum(per_done_month) as done_month
	from list_4
	left join lvs on (list_4.lecture_vt_no = lvs.lecture_vt_no and list_4.active_timestamp <= lvs.tutoring_datetime and if(list_4.done_timestamp is null, now(), done_timestamp) >= lvs.tutoring_datetime)
	where active_timestamp >= cast('2024-05-01' as timestamp)
	group by list_4.lecture_vt_no, group_lecture_vt_no, active_timestamp, done_timestamp, min_payment_no, updated_at
)
select * 
from list_5
where updated_at >= cast('{max_updatedat}' as timestamp)
'''


