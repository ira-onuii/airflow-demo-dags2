{{ config(
    materialized='table',
	schema='block_student'
) }}


WITH join_student_list AS (
    select u.user_No, now() as created_at
	from raw_data."user" u 
	left join raw_data.lecture_video_tutoring lvt on u.user_no = lvt.student_user_no 
	where lvt.student_user_no  is null 
	 )
SELECT *
    FROM join_student_list