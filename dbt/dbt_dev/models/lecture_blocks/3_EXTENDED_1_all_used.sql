{{ config(
    materialized='ephemeral'
) }}


WITH all_used_list AS (
    select lvt.lecture_vt_no 
	from raw_data.lecture_video_tutoring lvt 
	inner join raw_data.lecture_vt_schedules lvs on lvt.current_schedule_no = lvs.schedule_no 
	where lvt.stage_max_cycle_count = lvs.cycle_count 
)
SELECT *
    FROM WITH all_used_list
