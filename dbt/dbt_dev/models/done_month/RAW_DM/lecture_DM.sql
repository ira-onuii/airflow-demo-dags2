{{ config(
    materialized='incremental',
    schema='block_done-month',
    unique_key='lecture_teacher_vt_No'
) }}


WITH lecture_dm AS (
    select ltvt.lecture_teacher_vt_No, lvt.lecture_vt_no, lvt.student_user_No, lvt.tutoring_state, ltvt.teacher_user_no, ltvt.active_done_month, ltvt.total_done_month, now() as created_at, ltvt.create_at 
		from raw_data.lecture_teacher_vt ltvt
		inner join raw_data.lecture_video_tutoring lvt on ltvt.lecture_vt_no = lvt.lecture_vt_no 
		-- where lvts.per_done_month is not null 
)
SELECT lecture_teacher_vt_No, lecture_vt_no, student_user_No, tutoring_state, teacher_user_no, active_done_month, total_done_month, created_at
    FROM lecture_dm

{% if is_incremental() %}

  
  where lecture_dm.create_at > (select max(created_at) from {{ this }})

{% endif %}    