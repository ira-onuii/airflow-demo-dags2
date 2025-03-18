{{ config(
    materialized='table'
) }}


WITH teacher_active_dm AS (
    select ltvt.teacher_user_no, sum(ltvt.active_done_month) as teacher_active_dm, now() as created_at
		from {{ ref('lecture_DM') }} ltvt
        group by ltvt.teacher_user_no
	 )
SELECT *
    FROM teacher_active_dm