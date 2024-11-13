{{ config(
    materialized='ephemeral'
) }}


WITH regular_lecture_list AS (
    select *
        from {{ ref('lecture_DM') }}
        where activec_done_month >= 4
)
SELECT *
    FROM WITH regular_lecture_list
