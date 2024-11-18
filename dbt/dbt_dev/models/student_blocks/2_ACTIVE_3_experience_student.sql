{{ config(
    materialized='ephemeral'
) }}


WITH experience_student_list AS (
    select std.student_user_No
	from {{ ref('student_total_DM') }} std
	where std.student_total_dm < 4
    and std.student_state = 'ACTIVE'
	 )
SELECT *
    FROM experience_student_list