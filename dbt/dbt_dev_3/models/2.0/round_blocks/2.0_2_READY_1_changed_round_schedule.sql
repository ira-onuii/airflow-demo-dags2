{{ config(
    materialized='ephemeral'
) }}


WITH changed_round_schedule AS (
    select lvsh.history_No, lvsh.schedule_No, lvs.lecture_vt_No, lvsh.tutoring_datetime, lvsh.create_datetime, lvsh.chagne_type
        from raw_data.lecture_vt_schedules lvs
        inner join raw_data.lecture_vt_schedules_history lvsh on lvs.schedule_No = lvsh.schedule_No
)
SELECT *
    FROM WITH changed_round_schedule
