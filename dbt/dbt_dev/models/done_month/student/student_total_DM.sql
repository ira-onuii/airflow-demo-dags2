{{ config(
    materialized='table'
) }}


WITH student_total_dm AS (
    select ltvt.student_user_No, sum(ltvt.tutoring_total_dm) as student_total_dm
		from {{ ref('tutoring_total_DM') }} ltvt
        group by ltvt.student_user_No
	 )
SELECT *
    FROM student_total_dm