{{ config(
    materialized='ephemeral'
) }}


WITH pause_student_list AS (
    select u.user_No
	from raw_data."user" u 
	inner join raw_data.lecture_video_tutoring lvt on u.user_No = lvt.student_user_No
	where u.user_No not in 
		(select user_No 
			from {{ ref('INPROGRESS_in_progress_tutoring') }}
		)
	 )
SELECT *
    FROM pause_student_list