{{ config(
    materialized='table'
) }}


WITH tutoring_total_dm AS (
    select ltvt.lecture_vt_no, ltvt.student_user_No, sum(ltvt.total_done_month) as tutoring_total_dm
		from {{ ref('lecture_DM') }} ltvt
        group by ltvt.lecture_vt_No, ltvt.student_user_No
	 )
SELECT *
    FROM tutoring_total_dm