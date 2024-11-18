{{ config(
    materialized='ephemeral'
) }}


WITH changed_pause_tutoring_list AS (
    select tad.lecture_vt_No
        from {{ ref('4_PAUSE_0_pause_tutoring') }} pt
        inner join
            (select lcf.lecture_vt_no 
                from raw_data.lecture_change_form lcf 
                inner join raw_data.student_change_subject_v2 scsv on lcf.lecture_change_form_no = scsv.lecture_change_form_no 
                where lcf.process_status = '안내완료'
            ) lcf on pt.lecture_vt_No = lcf.lecture_vt_No
	 )
SELECT *
    FROM changed_pause_tutoring_list