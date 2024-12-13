{{ config(
    materialized='table',
    schema='block_lecture'
) }}


WITH first_lecture_list AS (
    select lecture_vt_no, student_user_No, tutoring_state, teacher_user_no, active_done_month, total_done_month, now() as created_at
        from {{ ref('lecture_DM') }}
        where total_done_month = 0
)
SELECT *
    FROM first_lecture_list
