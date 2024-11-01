{{ config(
    materialized='view'
) }}


WITH user_list AS (
    select *
		from {{ ref('student_metadata') }} smd
		where smd.user_No in
			(select user_No 
				from {{ ref('ACTIVE_experience_student') }} 
			)
 )
SELECT *
    FROM user_list