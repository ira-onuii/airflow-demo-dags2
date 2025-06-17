{{ config(
    materialized='incremental',
    unique_key='schedule_no',  
    on_schema_change='append_new_columns'
) }}

WITH update_existing AS (
    SELECT lvs.schedule_No, lvs.lecture_cycle_no, lvs.stage_count, lvs.cycle_count, lvs.update_datetime, cycle_payment_item
		, lvs.lecture_vt_No, sf.student_user_No, sf.teacher_user_no, lvs.per_done_month, now() as created_at
		from raw_data.lecture_vt_schedules lvs
		inner join raw_data.student_follow sf on lvs.follow_no = sf.follow_no
		where lvs.per_done_month is not null 
),


insert_new AS (
    SELECT schedule_No, lecture_cycle_no, stage_count, cycle_count, update_datetime, cycle_payment_item
		, lecture_vt_No, student_user_No, teacher_user_no, per_done_month, now() as created_at
    FROM update_existing s
    where NOT EXISTS (
        SELECT 1 
        FROM {{ this }} t
        WHERE t.schedule_No = s.schedule_No
    )
)

SELECT * FROM update_existing

{% if is_incremental() %}
UNION ALL 
SELECT * FROM insert_new
{% endif %}
