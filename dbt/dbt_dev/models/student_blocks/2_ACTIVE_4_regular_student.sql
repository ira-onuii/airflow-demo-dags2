{{ config(
    materialized='ephemeral'
) }}


WITH regular_student_list AS (
    select std.student_user_No
	from {{ ref('student_total_DM') }} std
	where std.student_total_dm >= 4
	 )
SELECT *
    FROM regular_student_list