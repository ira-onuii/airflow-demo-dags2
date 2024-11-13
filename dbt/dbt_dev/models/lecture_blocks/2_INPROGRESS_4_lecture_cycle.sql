{{ config(
    materialized='ephemeral'
) }}


WITH lecture_cycle_list AS (
    select lvt.lecture_vt_no, lvt.student_user_no, ltv.teacher_user_no, th.shelf_life
        from raw_data.lecture_teacher_vt ltv 
        inner join raw_data.lecture_video_tutoring lvt on ltv.lecture_vt_no = lvt.lecture_vt_no 
        inner join raw_data.tteok_ham th on lvt.payment_item = th.tteok_ham_no 
        where ltv.teacher_vt_status = 'ASSIGN'
)
SELECT *
    FROM WITH lecture_cycle_list
