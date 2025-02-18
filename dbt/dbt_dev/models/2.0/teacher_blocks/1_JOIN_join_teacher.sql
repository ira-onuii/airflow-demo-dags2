{{ config(
    materialized='ephemeral'
) }}


WITH join_teacher_list AS (
    select t.user_No
        from raw_data.teacher t
        left join raw_data.lecture_tutor lt on t.user_No = lt.user_No
        where t.seoltab_state = 'JOIN'
        and lt.user_No is null
	 )
SELECT *
    FROM join_teacher_list