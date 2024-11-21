{{ config(
    materialized='table',
	schema='block_student'
) }}


WITH reactive_first_student_list AS (
    select std.student_user_No, now() as created_at
	from {{ ref('student_active_DM') }} std
	where std.student_active_dm = 0
    and std.student_total_dm > 0
    and std.student_state = 'ACTIVE'
	 )
SELECT *
    FROM reactive_first_student_list