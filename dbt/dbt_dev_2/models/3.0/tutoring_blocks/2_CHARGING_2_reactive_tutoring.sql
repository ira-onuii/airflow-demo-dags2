{{ config(
    materialized='ephemeral'
) }}


WITH reactive_tutoring_list AS (
    select ttd.lecture_vt_No
	from {{ ref('tutoring_total_DM') }} ttd
    inner join {{ ref('tutoring_active_DM') }} tad on ttd.lecture_vt_No = tad.lecture_vt_No
	where ttd.tutoring_total_dm > 0
    and tad.tutoring_active_dm = 0
	 )
SELECT *
    FROM reactive_tutoring_list