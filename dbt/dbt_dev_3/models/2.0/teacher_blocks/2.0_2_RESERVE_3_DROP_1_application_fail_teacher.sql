{{ config(
    materialized='ephemeral'
) }}


WITH application_fail_teacher_list AS (
    select t.user_No
        from raw_data.teacher t
        left join raw_data.lecture_tutor lt on t.user_No = lt.user_No
        where t.seoltab_state = 'NOGOOD'
        and lt.user_No is not null
	 )
SELECT *
    FROM application_fail_teacher_list