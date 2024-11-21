{{ config(
    materialized='incremental',
    schema='kpis',
    incremental_strategy='append',
    unique_key='today'
) }}


with active_done as (
    select current_date as today
        , count(case when mtd.student_state = 'ACTIVE' and mtd.main_tutoring_active_dm = 0 then 1 else null end) as new_active
        , count(case when mtd.student_state = 'ACTIVE' and mtd.main_tutoring_active_dm between 0.001 and 3.999 then 1 else null end) as experience_active
        , count(case when mtd.student_state = 'ACTIVE' and mtd.main_tutoring_active_dm >= 4 then 1 else null end) as regular_active
        , count(case when mtd.student_state = 'DONE' then 1 else null end) as done
	    from {{ ref('main_tutoring_active_DM') }} mtd
),
new_first_student as (
    select current_date as today, count(*) as nfs_ct
        from {{ ref('2_ACTIVE_1_new_first_student') }} as nfs
        where to_char(nfs.created_at,'YYYY-MM-DD') = cast(current_date as varchar)
),
pause_student as (
    select current_date as today
        , count(case when ps.main_tutoring_active_dm = 0 then 1 else null end) as new_pause
        , count(case when ps.main_tutoring_active_dm between 0.001 and 3.999 then 1 else null end) as experience_pause
        , count(case when ps.main_tutoring_active_dm >= 4 then 1 else null end) as regular_pause
        from {{ ref('3_PAUSE_1_pause_student') }} as ps
        where to_char(ps.created_at,'YYYY-MM-DD') = cast(current_date as varchar)
),
reactive_student as (
    select current_date as today, count(*) as ras_ct
        from {{ ref('2_ACTIVE_2_reactive_first_student') }} as ras
        where to_char(ras.created_at,'YYYY-MM-DD') = cast(current_date as varchar)
)
select ad.today, ad.new_active, ad.experience_active, ad.regular_active, ad.done
    , nfs.nfs_ct, ps.new_pause, ps.experience_pause, ps.regular_pause, ras.ras_ct
    from active_done ad
    left join new_first_student nfs on ad.today = nfs.today
    left join pause_student ps on ad.today = ps.today
    left join reactive_student ras on ad.today = ras.today



