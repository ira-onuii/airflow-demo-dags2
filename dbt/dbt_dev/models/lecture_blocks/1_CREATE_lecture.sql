{{ config(
    materialized='ephemeral'
) }}


WITH create_lecture_list AS (
    select *
        from {{ ref('lecture_DM') }}
        where total_done_month is null
)
SELECT *
    FROM WITH create_lecture_list
