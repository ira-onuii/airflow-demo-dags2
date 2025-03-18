{{ config(
    materialized='ephemeral'
) }}


WITH change_matching_list AS (
    select m.suggestion_teacher_id, m.ms_updated_at, m.lecture_vt_No, m.student_id, m.subject
        from {{ ref('3_ACTIVE_1_MATCHING_2_accepted') }} m
        where m.type = 'TEACHER_CHANGE'
	 )
SELECT *
    FROM change_matching_list