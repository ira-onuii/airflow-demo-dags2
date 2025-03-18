{{ config(
    materialized='incremental',
    schema='block_done-month',
    incremental_strategy='merge',
    unique_key='lecture_teacher_vt_No'
) }}


WITH lecture_dm AS (
    select ltvt.lecture_teacher_vt_No, lvt.lecture_vt_no, lvt.student_user_No, lvt.tutoring_state, ltvt.teacher_user_no, ltvt.active_done_month, ltvt.total_done_month
        ,ltvt.create_at, ltvt.update_at 
		from raw_data.lecture_teacher_vt ltvt
		inner join raw_data.lecture_video_tutoring lvt on ltvt.lecture_vt_no = lvt.lecture_vt_no 
		-- where lvts.per_done_month is not null 
)
SELECT lecture_teacher_vt_No, lecture_vt_no, student_user_No, tutoring_state, teacher_user_no, active_done_month, total_done_month
    , {{ get_updated_at('ld', this, 'lecture_teacher_vt_No', 'now()') }} as updated_at
    FROM lecture_dm ld

{% if is_incremental() %}

  where ld.update_at > (select max(updated_at) from {{ this }})

{% endif %}    