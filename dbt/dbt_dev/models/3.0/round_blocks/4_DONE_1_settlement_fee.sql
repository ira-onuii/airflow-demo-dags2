{{ config(
    materialized='ephemeral'
) }}


WITH settlement_fee_list AS (
    select lvs.schedule_No, lvs.lecture_vt_No, lvs.tutoring_datetime, lvs.create_datetime, lvs.update_datetime
        , case when lvs.grade = 476 and ld.active_done_month > 6 then 20000*1.2
            when lvs.grade = 476 and ld.active_done_month > 4 and ld.active_done_month <= 6 then 20000*1.15
            when lvs.grade = 476 and ld.active_done_month > 2 and ld.active_done_month <= 4 then 20000*1.1
            when lvs.grade = 475 and ld.active_done_month > 2 then 20000*1.05
            else 20000 end as fee
        from 
            (select lvs.schedule_No, lvs.lecture_vt_No, lvs.tutoring_datetime, lvs.create_datetime, lvs.update_datetime
                , sf.teacher_user_No, s.grade
                from raw_data.lecture_vt_schedules lvs 
                inner join raw_data.student_follow sf on lvs.follow_no = sf.follow_no 
                inner join 
                    (select s.user_No, ttn.parent as grade
                        from raw_data.student s
                        inner join raw_data.term_taxonomy_name ttn on s."year" = ttn.term_taxonomy_id
                    ) s on sf.student_user_no = s.user_No
                where lvs.schedule_state = 'DONE'
            ) lvs
        inner join  {{ ref('lecture_DM') }} ld on (lvs.lecture_vt_No = ld.lecture_vt_No and lvs.teacher_user_No = ld.teacher_user_No)
)
SELECT *
    FROM WITH settlement_fee_list
