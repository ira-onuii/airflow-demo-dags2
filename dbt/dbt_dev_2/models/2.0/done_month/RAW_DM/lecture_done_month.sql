{{ config(
    materialized='incremental',
    unique_key='student_teacher_no',
    schema='block_done-month',
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
) }}

WITH update_existing AS (
    SELECT
        concat(rdm.student_user_no, rdm.teacher_user_no) AS student_teacher_no,
        rdm.student_user_no,
        rdm.teacher_user_no,
        SUM(per_done_month) AS lecture_done_month,
        MAX(update_datetime) AS max_update_datetime
    FROM {{ ref('round_done_month') }} rdm
    GROUP BY rdm.student_user_no, rdm.teacher_user_no
)

SELECT *
FROM update_existing

{% if is_incremental() %}
WHERE max_update_datetime >= (SELECT MAX(max_update_datetime) FROM {{ this }})
{% endif %}
