{{ config(
    materialized='table'
) }}


WITH lecture_dm AS (
    select lvt.lecture_vt_no, lvt.student_user_No, lvt.tutoring_state, ltvt.teacher_user_no, ltvt.active_done_month, ltvt.total_done_month 
		from raw_data.lecture_teacher_vt ltvt
		inner join raw_data.lecture_video_tutoring lvt on ltvt.lecture_vt_no = lvt.lecture_vt_no 
		-- where lvts.per_done_month is not null 
)
SELECT *
    FROM lecture_dm