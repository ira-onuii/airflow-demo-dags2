{{ config(
    materialized='table',
    schema='block_done-month'
) }}


WITH lecture_total_dm AS (
    select ltvt.lecture_vt_no, ltvt.student_user_No, ltvt.teacher_user_no, ltvt.total_done_month, now() as created_at 
		from {{ ref('lecture_DM') }} ltvt
	 )
SELECT *
    FROM lecture_total_dm