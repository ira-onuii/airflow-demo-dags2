{{ config(
    materialized='table',
    schema='block_lecture'
) }}


WITH all_used_list AS (
    select lvt.lecture_vt_no, now() as created_at 
	from raw_data.lecture_video_tutoring lvt 
	inner join raw_data.lecture_vt_schedules lvs on lvt.current_schedule_no = lvs.schedule_no 
	where lvt.stage_max_cycle_count = lvs.cycle_count 
)
SELECT *
    FROM all_used_list
