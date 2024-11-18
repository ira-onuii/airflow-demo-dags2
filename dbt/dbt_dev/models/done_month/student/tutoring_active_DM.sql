{{ config(
    materialized='table'
) }}


WITH tutoring_active_dm AS (
    select ltvt.lecture_vt_no, ltvt.student_user_No, ltvt.tutoring_state, sum(ltvt.active_done_month) as tutoring_active_dm
		from {{ ref('lecture_DM') }} ltvt
        group by ltvt.lecture_vt_no,ltvt.student_user_No, ltvt.tutoring_state
	 )
SELECT *
    FROM tutoring_active_dm