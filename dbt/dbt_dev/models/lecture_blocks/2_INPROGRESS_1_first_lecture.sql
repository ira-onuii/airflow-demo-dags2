{{ config(
    materialized='ephemeral'
) }}


WITH first_lecture_list AS (
    select *
        from {{ ref('lecture_DM') }}
        where total_done_month = 0
)
SELECT *
    FROM WITH first_lecture_list
