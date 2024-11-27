{{ config(
    materialized='table',
	schema='block_student'
) }}


WITH reactive_first_student_list AS (
    select sad.student_user_No, now() as created_at
	from {{ ref('student_active_DM') }} sad
    inner join {{ ref('student_total_DM') }} std on sad.student_user_No = std.student_user_No
	where sad.student_active_dm = 0
    and std.student_total_dm > 0
    and sad.student_state = 'ACTIVE'
	 )
SELECT *
    FROM reactive_first_student_list