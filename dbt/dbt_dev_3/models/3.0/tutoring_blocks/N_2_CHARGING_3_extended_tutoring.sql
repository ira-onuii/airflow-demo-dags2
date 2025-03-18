{{ config(
    materialized='ephemeral'
) }}


WITH extended_tutoring_list AS (
    select std.student_user_No
	from {{ ref('student_total_DM') }} std
	where std.student_total_dm = 0
	 )
SELECT *
    FROM extended_tutoring_list