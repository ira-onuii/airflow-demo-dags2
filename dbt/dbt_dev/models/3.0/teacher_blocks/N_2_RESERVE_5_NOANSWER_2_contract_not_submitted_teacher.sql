{{ config(
    materialized='ephemeral'
) }}


WITH no_answer_contract_not_submitted_teacher_list AS (
    select t.user_No
        from raw_data.teacher t
        left join raw_data.lecture_tutor lt on t.user_No = lt.user_No
        where t.seoltab_state = 'NPASS'
        and lt.user_No is not null
	 )
SELECT *
    FROM no_answer_contract_not_submitted_teacher_list