{{ config(
    materialized='ephemeral'
) }}


WITH finish_tutoring_list AS (
    select lvt.lecture_vt_No, lvt.student_user_No
        from raw_data.lecture_video_tutoring lvt
        where lvt.student_type in ('PAYED','PAYED_B')
        and lvt.tutoring_sate = 'FINISH'
	 )
SELECT *
    FROM finish_tutoring_list