{{ config(
    materialized='ephemeral'
) }}


WITH new_first_student_list AS (
    select std.student_user_No
	from {{ ref('student_total_DM') }} std
	where std.student_total_dm = 0
    and std.student_state = 'ACTIVE'
	 )
SELECT *
    FROM new_first_student_list