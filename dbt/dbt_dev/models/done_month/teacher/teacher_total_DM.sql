{{ config(
    materialized='table'
) }}


WITH teacher_total_dm AS (
    select ltvt.teacher_user_no, sum(ltvt.total_done_month) as teacher_total_dm
		from {{ ref('lecture_DM') }} ltvt
        group by ltvt.teacher_user_no
	 )
SELECT *
    FROM teacher_total_dm