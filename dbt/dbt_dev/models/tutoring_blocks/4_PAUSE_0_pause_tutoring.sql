{{ config(
    materialized='ephemeral'
) }}


WITH pause_tutoring_list AS (
    select lvt.lecture_vt_No
        from raw_data.lecture_video_tutoring lvt
        where lvt.student_type in ('PAYED','PAYED_B')
        and lvt.tutoring_sate in ('AUTO_FINISH','FINISH')
	 )
SELECT *
    FROM pause_tutoring_list