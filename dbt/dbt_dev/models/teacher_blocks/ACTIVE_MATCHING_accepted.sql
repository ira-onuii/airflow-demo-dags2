{{ config(
    materialized='ephemeral'
) }}


WITH matching_accepted_list AS (
    select m.suggestion_teacher_id, m.ms_updated_at
        from raw_data.matching m
        where m.ms_status = 'MATCHED'
	 )
SELECT *
    FROM join_teacher_list