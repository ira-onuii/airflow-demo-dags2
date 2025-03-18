{{ config(
    materialized='ephemeral'
) }}


WITH experience_tutoring_list AS (
    select tad.lecture_vt_No
        from {{ ref('tutoring_active_DM') }} tad
        where tad.tutoring_active_dm < 4
	 )
SELECT *
    FROM experience_tutoring_list