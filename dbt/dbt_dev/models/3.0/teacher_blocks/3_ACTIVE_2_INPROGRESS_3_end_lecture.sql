{{ config(
    materialized='ephemeral'
) }}


WITH end_lecture_list AS (
    select teacher_user_No, lecture_vt_No, teacher_vt_status, ltvt.create_at, ttn.name
        from raw_data.lecture_teacher_vt ltvt
        inner join raw_data.term_taxonomy_name ttn on ltvt.lecture_subject_id = ttn.term_taxonomy_id
        where ltvt.teacher_vt_status = 'UNASSIGN'
	 )
SELECT *
    FROM end_lecture_list