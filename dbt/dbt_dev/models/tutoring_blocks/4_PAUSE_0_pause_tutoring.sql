{{ config(
    materialized='ephemeral'
) }}


WITH pause_tutoring_list AS (
    select lvt.lecture_vt_No
        from raw_data.lecture_video_tutoring lvt
        where lvt.student_type in ('PAYED','PAYED_B')
        and lvt.tutoring_state in ('AUTO_FINISH','FINISH')
	 )
SELECT ptl.*, tad.tutoring_active_dm, now() as created_at 
    FROM pause_tutoring_list ptl
    inner join {{ ref('tutoring_active_DM') }} tad on ptl.lecture_vt_No = tad.lecture_vt_No