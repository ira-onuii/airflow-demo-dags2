{{ config(
    materialized='view',
    schema='kpis',
    unique_key='today'
) }}


with new_student as (
    select create_at, count(*) as new_ct
        from new_student
        group by created_at
),
pause_student as (
    select create_at, count(*) as pause_ct
        , count(if(main_done_month = 0,1,null)) as new_pause
        , count(if(main_done_month > 0 and main_done_month < 4,1,null)) as experience_pause
        , count(if(main_done_month >= 4,1,null)) as regular_pause
        from pause_student
        group by created_at
),
reactive_student as (
    select create_at, count(*) as reactive_ct
        from reactive_student
        group by created_at
),
leave_student as (
    select create_at, count(*) as leave_ct
        from leave_student
        group by created_at
),
experience_student as (
    select create_at, count(*) as experience_ct
        from experience_student
        group by created_at
),
regular_student as (
    select create_at, count(*) as regular_ct
        from regular_student
        group by created_at
),
student_indicator as (
    select rs.created_at, new_ct, pause_ct, reactive_ct, leave_ct, experience_ct, regular_ct
        from regular_student rs
        left join experience_student es on rs.created_at = es.created_at
        left join leave_student ls on rs.created_at = ls.created_at
        left join reactive_student ras on rs.created_at = ras.created_at
        left join pause_student ps on rs.created_at = ps.created_at
        left join new_student ns on rs.created_at = ns.created_at
)



