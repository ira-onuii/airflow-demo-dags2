{{ config(
    materialized='view'
) }}


WITH user_list AS (
    select *
		from {{ ref('student_metadata') }} smd
		where smd.user_No in
			(select user_No 
				from {{ ref('2_ACTIVE_3_experience_student') }} 
			)
 )
SELECT *
    FROM user_list