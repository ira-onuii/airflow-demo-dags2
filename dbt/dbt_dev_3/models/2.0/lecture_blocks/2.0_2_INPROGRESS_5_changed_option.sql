{{ config(
    materialized='table',
    schema='block_lecture'
) }}


WITH changed_option_list AS (
    select lcf.lecture_vt_No, lcf.create_datetime, lcf.update_datetime, now() as created_at
        from raw_data.lecture_change_form lcf
        where lcf.form_type = '옵션변경'
        and lcf.process_status = '안내완료'
)
SELECT *
    FROM changed_option_list
