{{ config(
    materialized='table'
) }}


WITH lecture_active_dm AS (
    select ltvt.student_user_No, sum(ltvt.active_done_month) as student_active_dm
		from {{ ref('lecture_DM') }} ltvt
        group by ltvt.student_user_No
	 )
SELECT *
    FROM lecture_active_dm