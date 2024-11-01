{{ config(
    materialized='table'
) }}


WITH lecture_active_dm AS (
    select ltvt.lecture_vt_no, ltvt.student_user_No, ltvt.teacher_user_no, ltvt.active_done_month 
		from {{ ref('lecture_DM') }} ltvt
	 )
SELECT *
    FROM lecture_active_dm