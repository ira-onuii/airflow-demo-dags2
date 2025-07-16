from datetime import datetime, timedelta

date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

from dimension_nps_7_under_total import max_updated_at

max_updatedat = max_updated_at()[0]

table_name = f'"{date}"'


group_lvt_query = f'''
-- 6점 이하 raw_lvt
with lvs as (
select row_number() over(partition by lecture_vt_no order by lvs.tutoring_datetime asc) as rn 
	, lvs.lecture_vt_no-- , gl.group_lecture_vt_no, gl.active_timestamp, gl.done_timestamp, gl.done_month
	, lvs.schedule_no, lvs.lecture_cycle_no, lvs.tutoring_datetime, lvs.schedule_state
	from mysql.onuei.lecture_vt_schedules lvs
	-- inner join data_warehouse.raw_data.group_lvt gl on (lvs.lecture_vt_no = gl.lecture_vt_no and lvs.tutoring_datetime between gl.active_timestamp and if(gl.done_timestamp is null,now(),gl.done_timestamp))
	where lvs.schedule_state = 'DONE'
),
fst_list as (
select *
	from lvs
	where lvs.rn = 1 
),
lsf as (
select fst_list.*, cast(concat(cast(lsf.student_id as varchar), cast(lsf.teacher_id as varchar)) as bigint) as student_teacher_No
	, lsf.feedback_cycle, lsf.student_id, lsf.teacher_id 
	from fst_list
	left join mysql.onuei.lecture_student_feedback lsf on fst_list.schedule_no = lsf.schedule_no 
),
fbd as (
select cast(concat(cast(student_user_id as varchar), cast(tutor_user_id as varchar)) as bigint) as student_teacher_no, cast(value as int) as fst_nps, created_at, lecture_vt_no 
	from 		
		(WITH cte AS (
			select sf.lecture_vt_no,sf.body_list_map,sf.cycle_count,sf.created_at,sf.tutor_user_id,sf.student_user_id 
				FROM marketing_scylladb.marketing_mbti.student_feedback_projects sf
				where sf.cycle_count=1
				)
				SELECT lecture_vt_no,cycle_count,created_at,tutor_user_id,student_user_id ,
					replace(replace(replace(concat_ws('', map_keys.key), '{{', ''), '}}', ''), '"', '') as key,replace(replace(replace(concat_ws('',map_keys.value),'{{',''),'}}',''),'"','') as value
					FROM cte
					CROSS JOIN UNNEST(split_to_multimap(cte.body_list_map, '","', ':')) map_keys(key, value)
		) A		
	where key in ('cyMj2Q5EbCE2gOZPuvJs','SbXI6fGKqJeuGagtJFqU','9qiFkIjQ4ztJE5vyy0rY','WwZI08GtA8p7KANNMTuJ')
)
,
lfbd as (
select lsf.lecture_vt_no, lsf.schedule_no, lsf.lecture_cycle_No
	, lsf.tutoring_datetime, lsf.schedule_state, lsf.student_id, lsf.teacher_id, fbd.created_at, fbd.fst_nps
	from lsf
	left join fbd on (lsf.student_teacher_No = fbd.student_teacher_No and lsf.lecture_vt_no  = fbd.lecture_vt_No)
)
select * 
	from lfbd 
    where fst_nps <= 6
	and created_at >= cast('{max_updatedat}' as timestamp)
'''


