{{ config(
    materialized='incremental',
	schema='block_student',
    incremental_strategy='merge',
    unique_key='student_user_No'
) }}


WITH new_first_student_list AS (
    select std.student_user_No
	from {{ ref('student_total_DM') }} std
	where std.student_total_dm = 0
    and std.student_state = 'ACTIVE'
	 )
SELECT 
    student_user_No,
    {{ get_created_at('nfsl', this, 'student_user_No', 'now()') }} as created_at
FROM new_first_student_list nfsl

{% if is_incremental() %}    
    where nfsl.student_user_No not in (select student_user_No from {{ this }})
{% endif %}

