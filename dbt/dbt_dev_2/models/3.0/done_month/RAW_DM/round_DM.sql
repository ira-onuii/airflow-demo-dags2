{{ config(
    materialized='table',
    schema='block_done-month'
) }}


with ifp as (
select ifp.id, cast(ifp.count_per_week as int) * 4 as count_per_month, ifp.subject
	from raw_data."item.fixed_package" ifp 
) 
, r as (
select lvr.lecture_cycle_id, lvr.updated_at, lvr.id as round_id
	from raw_data."lecture_v2.round" lvr
	where lvr.flow = 'MAIN'
	and lvr.status = 'END'
)
, lc as (
select lvlc.id, lvlc.lecture_id, lvlc.fixed_package_id 
	from raw_data."lecture_v2.lecture_cycle" lvlc 
)
, lu as (
select lu.lecture_id, lu.user_id
	from raw_data."lecture_v2.lecture_user" lu
	where lu.is_student = true
)
select r.round_id , lu.user_id, lc.lecture_id, ifp.subject, 1/cast(count_per_month as numeric) as round_done_month, r.updated_at
	from r
	inner join lc on r.lecture_cycle_id = lc.id
	inner join ifp on lc.fixed_package_id = ifp.id 
	left join lu on lc.lecture_id = lu.lecture_id 
	group by r.round_id , lu.user_id, lc.lecture_id, ifp.subject,1/cast(count_per_month as numeric), r.updated_at