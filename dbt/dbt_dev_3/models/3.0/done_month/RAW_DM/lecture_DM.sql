
{{ config(
    materialized='table',
    schema='block_done-month',
    unique_key='lecture_teacher_vt_No'
) }}


WITH lecture_dm AS (
   select user_id, lecture_id, "subject", lecture_status,sum(round_done_month) as lecture_done_month
    from  {{ ref('round_DM') }}
    group by user_id, lecture_id, lecture_status, "subject"
)
SELECT *
    from lecture_dm






