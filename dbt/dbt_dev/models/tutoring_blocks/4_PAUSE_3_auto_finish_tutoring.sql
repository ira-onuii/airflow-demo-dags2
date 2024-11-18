{{ config(
    materialized='ephemeral'
) }}


WITH auto_finish_tutoring_list AS (
    select lvt.lecture_vt_No
        from raw_data.lecture_video_tutoring lvt
        where lvt.student_type in ('PAYED','PAYED_B')
        and lvt.tutoring_sate = 'AUTO_FINISH'
	 )
SELECT *
    FROM auto_finish_tutoring_list