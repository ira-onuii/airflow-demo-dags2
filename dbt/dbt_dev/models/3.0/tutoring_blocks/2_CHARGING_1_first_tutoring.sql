{{ config(
    materialized='ephemeral'
) }}


WITH first_tutoring_list AS (
    select ttd.lecture_vt_No, now() as created_at 
	from {{ ref('tutoring_total_DM') }} ttd
	where ttd.tutoring_total_dm = 0
	 )
SELECT *
    FROM first_tutoring_list