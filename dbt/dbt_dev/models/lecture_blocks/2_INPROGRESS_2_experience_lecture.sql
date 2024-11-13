{{ config(
    materialized='ephemeral'
) }}


WITH experience_lecture_list AS (
    select *
        from {{ ref('lecture_DM') }}
        where activec_done_month < 4
)
SELECT *
    FROM WITH experience_lecture_list
