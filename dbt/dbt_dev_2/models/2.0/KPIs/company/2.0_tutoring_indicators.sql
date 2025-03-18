{{ config(
    materialized='incremental',
    schema='kpis',
    incremental_strategy='append',
    unique_key='today'
) }}


with active_done as (
    select current_date as today
        , count(case when ipt.tutoring_active_dm = 0 then 1 else null end) as new_active
        , count(case when ipt.tutoring_active_dm between 0.001 and 3.999 then 1 else null end) as experience_active
        , count(case when ipt.tutoring_active_dm >= 4 then 1 else null end) as regular_active
	    from {{ ref('3_INPROGRESS_0_in_progress_tutoring') }} ipt
),
new_first_tutoring as (
    select current_date as today, count(*) as nft_ct
        from {{ ref('2_CHARGING_1_first_tutoring') }} as nft
        where to_char(nft.created_at,'YYYY-MM-DD') = cast(current_date as varchar)
),
pause_tutoring as (
    select current_date as today
        , count(case when ps.tutoring_active_dm = 0 then 1 else null end) as new_pause
        , count(case when ps.tutoring_active_dm between 0.001 and 3.999 then 1 else null end) as experience_pause
        , count(case when ps.tutoring_active_dm >= 4 then 1 else null end) as regular_pause
        from {{ ref('4_PAUSE_0_pause_tutoring') }} as ps
        where to_char(ps.created_at,'YYYY-MM-DD') = cast(current_date as varchar)
),
changed_tutoring as (
    select current_date as today, count(*) as ras_ct
        from {{ ref('4_PAUSE_1_changed_pause_tutoring') }} as ras
        where to_char(ras.created_at,'YYYY-MM-DD') = cast(current_date as varchar)
)
select ad.today, ad.new_active, ad.experience_active, ad.regular_active
    , nft.nft_ct, ps.new_pause, ps.experience_pause, ps.regular_pause, ras.ras_ct
    from active_done ad
    left join new_first_tutoring nft on ad.today = nft.today
    left join pause_tutoring ps on ad.today = ps.today
    left join changed_tutoring ras on ad.today = ras.today



