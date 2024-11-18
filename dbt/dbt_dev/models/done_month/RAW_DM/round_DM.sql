{{ config(
    materialized='table'
) }}


WITH round_dm AS (
    select lvts.schedule_No, lvts.lecture_vt_no, lvt.student_user_No, lvt.tutoring_state, ltvt.teacher_user_no, lvts.per_done_month
		from raw_data.lecture_vt_schedules lvts
		inner join raw_data.lecture_video_tutoring lvt on lvts.lecture_vt_no = lvt.lecture_vt_no 
		inner join raw_data.lecture_teacher_vt ltvt on lvts.lecture_vt_No = ltvt.lecture_vt_No
		where lvts.per_done_month is not null 
	 )
SELECT *
    FROM round_dm