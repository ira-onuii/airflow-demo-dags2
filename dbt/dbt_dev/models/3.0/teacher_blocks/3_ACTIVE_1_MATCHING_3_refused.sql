{{ config(
    materialized='ephemeral'
) }}


WITH matching_refused_list AS (
    select m.suggestion_teacher_id, m.ms_updated_at, m.type, m.lecture_vt_No, m.student_id, m.subject
        from raw_data.matching m
        where m.ms_status = 'REFUSED'
	 )
SELECT *
    FROM matching_refused_list