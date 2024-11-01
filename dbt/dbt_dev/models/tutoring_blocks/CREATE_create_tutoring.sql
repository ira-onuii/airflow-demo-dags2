{{ config(
    materialized='ephemeral'
) }}


WITH create_tutoring_list AS (
    select lvt.lecture_vt_No
	from raw_data.lecture_video_tutoring lvt
	where lvt.student_type in ('PAYED','PAYED_B')
    and lvt.tutoring_state in ('REGISTER','REMATCH','REMATCH_B')
	 )
SELECT *
    FROM create_tutoring_list