{{ config(
    materialized='table'
) }}


WITH main_tutoring_total_dm AS (
    select ltvt.lecture_vt_no, ltvt.student_user_No, max(ltvt.tutoring_total_dm) as main_tutoring_total_dm
		from {{ ref('tutoring_total_DM') }} ltvt
        group by ltvt.student_user_No
	 )
SELECT *
    FROM main_tutoring_total_dm