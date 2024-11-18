{{ config(
    materialized='table'
) }}


WITH student_total_dm AS (
    select ltvt.student_user_No
        case when count(case when ltvt.tutoring_state not in ('FINISH','AUTO_FINISH','DONE') then 1 else null end) > 0 then 'ACTIVE' else 'DONE' end as student_state
        , sum(ltvt.tutoring_total_dm) as student_total_dm
		from {{ ref('tutoring_total_DM') }} ltvt
        group by ltvt.student_user_No
	 )
SELECT *
    FROM student_total_dm