{{ config(
    materialized='incremental',
    unique_key='schedule_no',  
    schema='block_done-month',
    on_schema_change='append_new_columns'
) }}

WITH update_existing AS (
    SELECT 
        lvs.schedule_No, lvs.lecture_cycle_no, lvs.stage_count, lvs.cycle_count,
        lvs.update_datetime, cycle_payment_item,
        lvs.lecture_vt_No, sf.student_user_No, sf.teacher_user_no, round(lvs.per_done_month,3) as per_done_month,
        now() as created_at
    FROM raw_data.lecture_vt_schedules lvs
    INNER JOIN raw_data.student_follow sf ON lvs.follow_no = sf.follow_no
    WHERE lvs.per_done_month IS NOT NULL
)

{% if is_incremental() %}

SELECT *
FROM update_existing s
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} t
    WHERE t.schedule_No = s.schedule_No
)

{% else %}

SELECT *
FROM update_existing

{% endif %}
