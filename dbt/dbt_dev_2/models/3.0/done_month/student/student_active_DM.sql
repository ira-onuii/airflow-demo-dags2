{{ config(
    materialized='table',
    schema='block_done-month'
) }}


WITH student_active_dm AS (
    select ltvt.student_user_No
        , case when count(case when ltvt.tutoring_state not in ('FINISH','AUTO_FINISH','DONE') then 1 else null end) > 0 then 'ACTIVE' else 'DONE' end as student_state
        , sum(ltvt.active_done_month) as student_active_dm, now() as updated_at
		from {{ ref('lecture_DM') }} ltvt
        group by ltvt.student_user_No
	 )
SELECT *
    FROM student_active_dm