{{ config(
    materialized='table',
    schema='block_lecture'
) }}


WITH auto_finish_lecture_list AS (
    select lvt.lecture_vt_no, lvt.student_user_No, ltv.teacher_user_No, now() as created_at
	from {{ ref('4_PAUSE_3_auto_finish_tutoring') }} lvt
	inner join 
        (select lecture_vt_No, teacher_user_no
	        from 
                (select row_number() over(partition by ltv.lecture_vt_no order by ltv.update_at desc) as rn 
                    , lecture_vt_No, teacher_user_no
                    from raw_data.lecture_teacher_vt ltv 
                    where ltv.teacher_vt_status = 'UNASSIGN'
                ) A
            where A.rn = 1
	    ) ltv on lvt.lecture_vt_no = ltv.lecture_vt_No
)
SELECT *
    FROM auto_finish_lecture_list
