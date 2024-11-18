{{ config(
    materialized='ephemeral'
) }}


WITH reactive_first_student_list AS (
    select std.student_user_No
	from {{ ref('student_active_DM') }} std
	where std.student_active_dm = 0
    and std.student_state = 'ACTIVE'
	 )
SELECT *
    FROM reactive_first_student_list