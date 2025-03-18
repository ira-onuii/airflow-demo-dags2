{{ config(
    materialized='incremental',
    schema='block_student',
    incremental_strategy='merge',
    unique_key='student_user_No'
) }}


WITH pause_student_list AS (
    select u.user_No as student_user_No, now() as created_at
	from raw_data."user" u 
	inner join raw_data.lecture_video_tutoring lvt on u.user_No = lvt.student_user_No
	where u.user_No not in 
		(select user_No 
			from {{ ref('3_INPROGRESS_0_in_progress_tutoring') }}
		)
	 )
SELECT 
    psl.student_user_No,
	mtad.main_tutoring_active_DM,
	{{ get_created_at('psl', this, 'student_user_No', 'now()') }} as created_at
FROM pause_student_list psl
inner join {{ ref('main_tutoring_active_DM') }} mtad on psl.student_user_No = mtad.student_user_No


{% if is_incremental() %}    
    where psl.student_user_No not in (select student_user_No from {{ this }})
{% endif %}