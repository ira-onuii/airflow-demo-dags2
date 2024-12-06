{{ config(
    materialized='ephemeral'
) }}


WITH in_progress_tutoring_list AS (
    select lvt.lecture_vt_No, lvt.student_user_No, lvt.create_datetime
        from raw_data.lecture_video_tutoring lvt
        where lvt.student_type in ('PAYED','PAYED_B')
        and lvt.tutoring_state in ('REMATCH','REMATCH_B','MATCHED','ACTIVE')
	 )
SELECT iptl.*, tad.tutoring_active_dm
    FROM in_progress_tutoring_list iptl
    inner join {{ ref('tutoring_active_DM') }} tad on iptl.lecture_vt_No = tad.lecture_vt_No