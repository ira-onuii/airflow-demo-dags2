{{ config(
    materialized='ephemeral'
) }}


WITH matching_refused_list AS (
    select m.suggestion_teacher_id, m.ms_updated_at, refusedreason
        from raw_data.matching m
        where m.ms_status = 'REFUSED'
	 )
SELECT *
    FROM join_teacher_list