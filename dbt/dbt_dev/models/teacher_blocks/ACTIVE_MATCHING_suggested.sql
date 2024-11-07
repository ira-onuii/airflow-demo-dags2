{{ config(
    materialized='ephemeral'
) }}


WITH matching_suggestion_list AS (
    select m.suggestion_teacher_id, m.suggestedat
        from raw_data.matching m
	 )
SELECT *
    FROM join_teacher_list