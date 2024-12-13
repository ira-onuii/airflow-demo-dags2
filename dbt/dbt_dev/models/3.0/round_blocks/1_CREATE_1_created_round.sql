{{ config(
    materialized='ephemeral'
) }}


WITH created_round_list AS (
    select lvs.schedule_No, lvs.lecture_vt_No, lvs.tutoring_datetime, lvs.create_datetime, lvs.update_datetime
        from raw_data.lecture_vt_schedules lvs
        where lvs.schedule_state in ('CONTRACT','RESERVATION')
)
SELECT *
    FROM WITH created_round_list
