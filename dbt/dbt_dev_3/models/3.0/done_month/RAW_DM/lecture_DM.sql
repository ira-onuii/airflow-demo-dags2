
{{ config(
    materialized='table',
    schema='block_done-month',
    unique_key='lecture_id'
) }}


WITH lecture_dm AS (
   select user_id, lecture_id, "subject", l.status as lecture_status, sum(round_done_month) as lecture_done_month
    from  {{ ref('round_DM') }} r
    inner join raw_data."lecture_v2.lecture" l on r.lecture_id = l.id
    group by user_id, lecture_id, l.status, "subject"
)
SELECT *
    from lecture_dm






