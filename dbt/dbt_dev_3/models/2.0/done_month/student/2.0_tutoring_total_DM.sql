{{ config(
    materialized='table',
    schema='block_done-month'
) }}


WITH tutoring_total_dm AS (
    select ltvt.lecture_vt_no, ltvt.student_user_No, ltvt.tutoring_state, sum(ltvt.total_done_month) as tutoring_total_dm, now() as created_at
		from {{ ref('lecture_DM') }} ltvt
        group by ltvt.lecture_vt_No, ltvt.student_user_No, ltvt.tutoring_state
	 )
SELECT *
    FROM tutoring_total_dm