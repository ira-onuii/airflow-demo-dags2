{{ config(
    materialized='ephemeral'
) }}


WITH contract_education_teacher_list AS (
    select t.user_No
        from raw_data.teacher t
        left join raw_data.lecture_tutor lt on t.user_No = lt.user_No
        where t.seoltab_state = 'GPASS'
        and lt.user_No is not null
	 )
SELECT *
    FROM contract_education_teacher_list