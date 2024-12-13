{{ config(
    materialized='table',
    schema='block_done-month'
) }}


WITH main_tutoring_total_dm AS (
    select ltvt.student_user_No
    , case when count(case when ltvt.tutoring_state not in ('FINISH','AUTO_FINISH','DONE') then 1 else null end) > 0 then 'ACTIVE' else 'DONE' end as student_state
    , max(ltvt.tutoring_total_dm) as main_tutoring_total_dm
        group by ltvt.student_user_No
	 )
SELECT *
    FROM main_tutoring_total_dm