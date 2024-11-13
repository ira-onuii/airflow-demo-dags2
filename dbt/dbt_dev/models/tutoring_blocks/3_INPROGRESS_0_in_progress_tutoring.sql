{{ config(
    materialized='ephemeral'
) }}


WITH in_progress_tutoring_list AS (
    select lvt.lecture_vt_No
        from raw_data.lecture_video_tutoring lvt
        where lvt.student_type in ('PAYED','PAYED_B')
        and lvt.tutoring_state in ('REMATCH','REMATCH_B','MATCHED','ACTIVE')
	 )
SELECT *
    FROM in_progress_tutoring_list