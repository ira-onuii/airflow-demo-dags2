{{ config(
    materialized='incremental',
	schema='block_student',
    incremental_strategy='merge',
    unique_key='student_user_No'
) }}


WITH reactive_first_student_list AS (
    select sad.student_user_No, now() as created_at
	from {{ ref('student_active_DM') }} sad
    inner join {{ ref('student_total_DM') }} std on sad.student_user_No = std.student_user_No
	where sad.student_active_dm = 0
    and std.student_total_dm > 0
    and sad.student_state = 'ACTIVE'
	 )
SELECT 
    student_user_No,
    {{ get_created_at('rfsl', this, 'student_user_No', 'now()') }} as created_at
FROM reactive_first_student_list rfsl

{% if is_incremental() %}    
    where nfsl.student_user_No not in (select student_user_No from {{ this }})
{% endif %}