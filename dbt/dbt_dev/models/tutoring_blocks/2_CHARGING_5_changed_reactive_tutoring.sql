{{ config(
    materialized='ephemeral'
) }}


WITH changed_reactive_tutoring_list AS (
    select ft.*  
        from {{ ref('CHARGING_reactive_tutoring') }} ft
        inner join 
            (select lvt.lecture_vt_no,A.student_user_No	
                    from 
                        (select scs.selected_subject, lvt.student_user_no ,lcf.lecture_change_form_no 
                            from raw_data.student_change_subject_v2 scs
                            inner join raw_data.lecture_change_form lcf on scs.lecture_change_form_no  = lcf.lecture_change_form_no 
                            inner join raw_data.lecture_video_tutoring lvt on lcf.lecture_vt_no = lvt.lecture_vt_no
                        ) A
                    inner join raw_data.lecture_video_tutoring lvt on (A.student_user_No = lvt.student_user_no and A.selected_subject = cast(lvt.lecture_subject_id as varchar))
            ) sc on ft.lecture_vt_No = sc.lecture_vt_No
)
SELECT *
    FROM changed_reactive_tutoring_list