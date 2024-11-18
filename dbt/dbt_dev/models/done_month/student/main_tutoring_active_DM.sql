{{ config(
    materialized='table'
) }}


WITH lecture_active_dm AS (
    select ltvt.lecture_vt_no, ltvt.student_user_No, sum(ltvt.active_done_month) as tutoring_active_dm
		from {{ ref('lecture_DM') }} ltvt
        group by ltvt.lecture_vt_no,ltvt.student_user_No
	 )
SELECT *
    FROM lecture_active_dm