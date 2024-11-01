{{ config(
    materialized='ephemeral'
) }}


WITH join_teacher_list AS (
    select *
        from    
            (select row_number() over(partition by t.user_No order by lt.create_datetime) as rn, t.user_No
                from raw_data.teacher t
                left join raw_data.lecture_tutor lt on t.user_No = lt.user_No
                where t.seoltab_state = 'JOIN'
                and lt.user_No is not null
            ) t
        where t.rn = 1
	 )
SELECT *
    FROM join_teacher_list