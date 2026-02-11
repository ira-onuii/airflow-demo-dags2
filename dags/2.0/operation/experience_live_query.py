from datetime import datetime, timedelta

date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

raw_sheet_query = '''
-- 책임 수업 & 매칭 제도 대상자 추출
with 
    glvt_all as (
            select glvt.group_lecture_vt_no, glvt.lecture_vt_no, glvt.active_timestamp,
                    LEAD(active_timestamp) OVER (PARTITION BY lecture_vt_no ORDER BY active_timestamp) AS next_active_timestamp
                    from data_warehouse.raw_data.group_lvt glvt
                ),
    lvts as (
            select lvt.lecture_vt_no, u.user_no, u.name, u.phone_number, u.email_id, lvt.tutoring_state, ttn.name as subject
            from mysql.onuei.lecture_video_tutoring lvt
            inner join mysql.onuei.user u on lvt.student_user_no = u.user_no
            left join mysql.onuei.term_taxonomy_name ttn on lvt.lecture_subject_id = ttn.term_taxonomy_id
            where lvt.student_type not in ('CTEST')
          ),
    t as (
            select te.user_no as teacher_user_no, ut.name as t_name
                    from mysql.onuei.teacher te
                    left join mysql.onuei.user ut on te.user_no = ut.user_no
                    where te.seoltab_state is not null
                    and te.lecture_phone_number not in ('01000000000','01099999999')
            ),
    ltvt_new_base as (
            select ltvt.lecture_teacher_vt_no, ltvt.lecture_vt_no, ltvt.teacher_user_no, ltvt.teacher_vt_status, ltvt.total_done_month, ltvt.create_at
                    from mysql.onuei.lecture_teacher_vt ltvt
                    where ltvt.create_at >= timestamp '2026-01-21 00:00:00'
            ),
    ltvt_new_mapped as (
            select g.group_lecture_vt_no, g.active_timestamp, g.next_active_timestamp, nb.lecture_teacher_vt_no, nb.lecture_vt_no, nb.teacher_user_no, nb.teacher_vt_status, nb.total_done_month, nb.create_at
                    from ltvt_new_base nb
                    join glvt_all g on nb.lecture_vt_no = g.lecture_vt_no
                       and nb.create_at >= g.active_timestamp
                       and (g.next_active_timestamp is null or nb.create_at < g.next_active_timestamp)
            ),
    sch_new as (
            select * from (
                select nm.group_lecture_vt_no, nm.lecture_teacher_vt_no, lvs.lecture_vt_no, lvs.schedule_no, lvs.create_datetime, lvs.tutoring_datetime, lvs.update_datetime, lvs.schedule_state, sf.teacher_user_no, sf.follow_no, lvs.lecture_cycle_no, sf.student_user_no,
                        ROW_NUMBER() OVER (PARTITION BY nm.lecture_teacher_vt_no ORDER BY lvs.create_datetime ASC, lvs.schedule_no ASC) AS schedule_rank,
                        SUM(CASE WHEN lvs.schedule_state = 'DONE' THEN 1 ELSE 0 END) OVER (PARTITION BY nm.lecture_teacher_vt_no ORDER BY lvs.create_datetime ASC, lvs.schedule_no ASC) AS done_rank
                        from ltvt_new_mapped nm
                        join mysql.onuei.lecture_vt_schedules lvs on nm.lecture_vt_no = lvs.lecture_vt_no
                            and lvs.create_datetime >= nm.create_at
                        join mysql.onuei.student_follow sf on lvs.follow_no = sf.follow_no
                           and sf.teacher_user_no = nm.teacher_user_no
            ) x
            where schedule_rank <= 4
        ),
    feedback as (
            select concat(cast(lsf.lecture_vt_no as varchar),'_',cast(lsf.teacher_id as varchar),'_',cast(lsf.feedback_cycle as varchar))as key_no, lsf.schedule_no
                    from mysql.onuei.lecture_student_feedback lsf
                    where lsf.created_at >= timestamp '2025-11-01 00:00:00'
                    and feedback_cycle <= 3
            ),
    nps as (
            select concat(cast(A.lecture_vt_no as varchar),'_',cast(A.tutor_user_id as varchar),'_',cast(A.cycle_count as varchar))as key_no,
                    date_format(A.created_at + interval '9' hour, '%Y-%m-%d %H:%i:%s') as "제출일시"
                    ,max(CASE WHEN A.key in ('"cycle01_01_hello"','"cycle02_01_ready"','"cycle03_01_ready"') THEN A.value END) AS "1번"
                    ,max(CASE WHEN A.key in ('"cycle01_02_promise"','"cycle02_02_question"','"cycle03_02_question"') THEN A.value END) AS "2번"
                    ,max(CASE WHEN A.key in ('"cycle01_03_question"','"cycle02_03_compliment"','"cycle03_03_compliment"') THEN A.value END) AS "3번"
                    ,max(CASE WHEN A.key in ('"cycle01_04_monthlyplan"','"cycle02_04_summary"','"cycle03_04_summary"') THEN A.value END) AS "4번"
                    ,max(CASE WHEN A.key in ('"cycle01_05_respect"','"cycle02_05_respect"','"cycle03_05_respect"') THEN A.value END) AS "5번"
                    ,max(CASE WHEN A.key in ('"cyMj2Q5EbCE2gOZPuvJs"','"SbXI6fGKqJeuGagtJFqU"','"9qiFkIjQ4ztJE5vyy0rY"') THEN A.value END) AS "선생님 추천 점수"
                    ,max(CASE WHEN A.key in ('"V3m8LmkMKv265jK0gqGR"','"CO6txyeWj4VL8EQIrkgd"','"WuBLJuA2fRJPHxfp0W3i"') THEN A.value END) AS "중립_개선점"
                    ,max(CASE WHEN A.key in ('"cycle01_under_8_change_tutor"','"cycle02_under_8_change_tutor"','"cycle03_under_8_change_tutor"') THEN A.value END) AS "중립_선생님 변경 희망 여부"
                    ,max(CASE WHEN A.key in ('"y6SQ8lSRfrrhzusiamMo"','"dKuaT9i0aX76oV3wbr6C"','"30y5Fl6sTpwI38EVY41g"') THEN A.value END) AS "비추천_개선점"
                    ,max(CASE WHEN A.key in ('"cycle01_under_6_change_tutor"','"cycle02_under_6_change_tutor"','"cycle03_under_6_change_tutor"') THEN A.value END) AS "비추천_선생님 변경 희망 여부"
                    from (
                            select lecture_vt_no, cycle_count, created_at, tutor_user_id, student_user_id,
                                            replace(replace(concat_ws('',map_keys.key),'{',''),'}','') as key,
                                            replace(replace(replace(concat_ws('',map_keys.value),'{',''),'}',''),'"','') as value
                                    from (
                                        select * from (
                                            select row_number() over(partition by sf.lecture_vt_no, sf.tutor_user_id, sf.cycle_count order by sf.created_at desc) as rn, sf.*
                                                    from marketing_scylladb.marketing_mbti.student_feedback_projects sf
                                                    where sf.created_at > timestamp '2025-11-01 00:00:00'
                                                    and sf.cycle_count <= 3
                                        ) x
                                        where x.rn = 1
                                                ) nps_raw
                                CROSS JOIN UNNEST(split_to_multimap(nps_raw.body_list_map, '",', ':')) map_keys(key, value)
                            ) A
            group by A.lecture_vt_no,A.tutor_user_id,A.cycle_count,A.created_at
            ),
    link as (
            select s.schedule_no, lvc.durations
                    from sch_new s
                left join mysql.onuei.lecture_vt_cycles lvc on s.lecture_cycle_no = lvc.lecture_cycle_no
        ),
    matchingdata as (
            select g.group_lecture_vt_no, g.lecture_vt_no, md.tutor_id, md.matchedat,
                    ROW_NUMBER() OVER (PARTITION BY g.group_lecture_vt_no, md.tutor_id ORDER BY md.matchedat ASC) AS rn
                    from glvt_all g
                    left join (
                        select mlvt.lectures[1].id AS lecture_id,
                               date_add('hour', 9, mlvt.matchedat) as matchedat,
                               mlvt.matchedteacher.id as tutor_id
                                 from matching_mongodb.matching.matching_lvt mlvt
                                 where date_add('hour', 9, mlvt.matchedat) >= cast('2026-01-21 00:00:00' as timestamp)
                        ) md on g.lecture_vt_no = md.lecture_id and g.active_timestamp < md.matchedat
            ),
    ticket as (
            select * from (
                select cst.lecture_vt_no, cst.update_datetime, substr(cst.content, 34, 3) as tname,
                       row_number() over (partition by cst.lecture_vt_no, substr(cst.content, 34, 3) order by cst.update_datetime desc) as rn
                from mysql.onuei.customer_service_ticket cst
                where cst.update_datetime > timestamp '2025-11-01 00:00:00'
                  and cst.content like ('%첫 메시지 도착%')
            ) sub
            where rn = 1
            )
select nm.group_lecture_vt_no,
        sn.create_datetime,
        fb.key_no,
        lv.tutoring_state,
        sn.lecture_vt_no,
        lv.subject as "과목",
        sn.schedule_state,
        case
            when sn.done_rank = 0 and sn.schedule_state in ('RESERVATION','CONTRACT') then '첫 수업 전'
            when sn.done_rank = 1 and sn.schedule_state in ('DONE') then '1회차 완료'
            when sn.done_rank = 1 and sn.schedule_state in ('RESERVATION') then '2회차 전'
            when sn.done_rank = 2 and sn.schedule_state in ('DONE') then '2회차 완료'
            when sn.done_rank = 2 and sn.schedule_state in ('RESERVATION') then '3회차 전'
            when sn.done_rank = 3 and sn.schedule_state in ('DONE') then '3회차 완료'
            when sn.done_rank = 3 and sn.schedule_state not in ('DONE') then '종료'
            when sn.done_rank >= 4 then '종료'
            when sn.schedule_state in ('TUTORING') then '수업 중'
            when sn.schedule_state in ('CANCEL') then '취소'
            else ' '
        end as "회차",
        nm.active_timestamp,
        lv.user_no,
        lv.name,
        lv.phone_number,
        t.teacher_user_no,
        t.t_name,
        sn.tutoring_datetime,
        case
            when sn.schedule_state in ('DONE') then sn.update_datetime
            else null
        end as "마치기 시점",
        nps."제출일시",
        nps."선생님 추천 점수",
        nps."1번",
        nps."2번",
        nps."3번",
        nps."4번",
        nps."5번",
        case
            when nps."중립_개선점" is not null then nps."중립_개선점"
            when nps."비추천_개선점" is not null then nps."비추천_개선점"
            else '없음'
        end as "개선점",
        case
            when nps."중립_선생님 변경 희망 여부" in ('yes', 'yes_timechange') then nps."중립_선생님 변경 희망 여부"
            when nps."비추천_선생님 변경 희망 여부" in ('yes', 'yes_timechange') then nps."비추천_선생님 변경 희망 여부"
            else 'X'
        end as "변경희망여부",
        md.matchedat,
        nm.total_done_month,
        lk.durations,
        case
            when date_diff('hour', md.matchedat, tk.update_datetime) >= 24 then '초과'
            when date_diff('hour', md.matchedat, tk.update_datetime) < 24 then '통과'
            when sn.schedule_state not in ('DONE') or sn.schedule_state is null then ''
            else '확인 필요'
        end as "위반",
        nm.teacher_vt_status
from ltvt_new_mapped nm
join sch_new sn on nm.lecture_teacher_vt_no = sn.lecture_teacher_vt_no
left join lvts lv on sn.lecture_vt_no = lv.lecture_vt_no
left join t on sn.teacher_user_no = t.teacher_user_no
left join feedback fb on sn.schedule_no = fb.schedule_no
left join nps on nps.key_no = fb.key_no
left join link lk on lk.schedule_no = sn.schedule_no
left join matchingdata md on nm.group_lecture_vt_no = md.group_lecture_vt_no
   and md.tutor_id = sn.teacher_user_no and md.rn = 1
left join ticket tk on sn.lecture_vt_no = tk.lecture_vt_no and tk.tname = t.t_name
where lv.name not like ('%테스트%')
  and lv.phone_number not like ('%00000000%')
  and lv.email_id not like ('%@seoltab.test%')
  and (case when sn.schedule_state in ('DONE') then sn.update_datetime else null end) is not null
  and sn.create_datetime >= timestamp '2026-01-21 00:00:00'
  and md.matchedat >= timestamp '2026-01-21 00:00:00'
order by "마치기 시점" asc, sn.create_datetime asc, sn.lecture_vt_no asc
'''


monitoring_sheet_query = '''
-- 추출 쿼리
with 
    glvt_all as (
            select glvt.group_lecture_vt_no,glvt.lecture_vt_no , glvt.active_timestamp,
                    LEAD(glvt.active_timestamp) OVER (PARTITION BY glvt.lecture_vt_no ORDER BY glvt.active_timestamp) AS next_active_timestamp
                    from data_warehouse.raw_data.group_lvt glvt
                ),
    lvts as (
            select lvt.lecture_vt_no, u.user_no, u.name, u.phone_number, u.email_id, s.parent_name, s.parent_phone_number, lvt.tutoring_state, ttn.name as subject, lvt.lecture_subject_id
            from mysql.onuei.lecture_video_tutoring lvt
            join mysql.onuei.user u on lvt.student_user_no = u.user_no
            left join mysql.onuei.student s on lvt.student_user_no = s.user_no
            left join mysql.onuei.term_taxonomy_name ttn on lvt.lecture_subject_id = ttn.term_taxonomy_id
            where lvt.student_type not in ('CTEST')
          ),
    t as (
            select te.user_no as teacher_user_no, ut.name as t_name, te.lecture_phone_number
                    from mysql.onuei.teacher te
                    left join mysql.onuei.user ut on te.user_no = ut.user_no 
                    where te.seoltab_state is not null
                    and te.lecture_phone_number not in ('01000000000','01099999999')
            ),
    ltvt_new_base as (
            select ltvt.lecture_teacher_vt_no, ltvt.lecture_vt_no, ltvt.teacher_user_no, ltvt.teacher_vt_status, ltvt.total_done_month, ltvt.create_at
                    from mysql.onuei.lecture_teacher_vt ltvt
                    where ltvt.create_at >= timestamp '2026-01-21 00:00:00'
            ),
    ltvt_new_mapped as (
            select g.group_lecture_vt_no, g.active_timestamp, g.next_active_timestamp, nb.lecture_teacher_vt_no, nb.lecture_vt_no, nb.teacher_user_no, nb.teacher_vt_status, nb.total_done_month, nb.create_at
                    from ltvt_new_base nb
                    join glvt_all g on nb.lecture_vt_no = g.lecture_vt_no
                       and nb.create_at >= g.active_timestamp
                       and (g.next_active_timestamp is null or nb.create_at < g.next_active_timestamp)
            ),
    sch_new as (
            select * from (
                select nm.group_lecture_vt_no, nm.lecture_teacher_vt_no, lvs.lecture_vt_no, lvs.schedule_no, lvs.create_datetime, lvs.tutoring_datetime, lvs.update_datetime, lvs.schedule_state, sf.teacher_user_no, sf.follow_no, lvs.lecture_cycle_no, sf.student_user_no,
                        ROW_NUMBER() OVER (PARTITION BY nm.lecture_teacher_vt_no ORDER BY lvs.create_datetime ASC, lvs.schedule_no ASC) AS schedule_rank,
                        SUM(CASE WHEN lvs.schedule_state = 'DONE' THEN 1 ELSE 0 END)
                            OVER (PARTITION BY nm.lecture_teacher_vt_no ORDER BY lvs.create_datetime ASC, lvs.schedule_no ASC) AS done_rank
                        from ltvt_new_mapped nm
                        join mysql.onuei.lecture_vt_schedules lvs on nm.lecture_vt_no = lvs.lecture_vt_no 
                            and lvs.create_datetime >= nm.create_at
                        join mysql.onuei.student_follow sf on lvs.follow_no = sf.follow_no
                           and sf.teacher_user_no = nm.teacher_user_no
            ) x
            where schedule_rank <= 4
        ),
    feedback as (
            select concat(cast(lsf.lecture_vt_no as varchar),'_',cast(lsf.teacher_id as varchar),'_',cast(lsf.feedback_cycle as varchar))as key_no, lsf.schedule_no
                    from mysql.onuei.lecture_student_feedback lsf 
                    where lsf.created_at >= timestamp '2025-11-01 00:00:00'
                    and feedback_cycle <= 3
            ),
    nps as (
            select
                    concat(cast(A.lecture_vt_no as varchar),'_',cast(A.tutor_user_id as varchar),'_',cast(A.cycle_count as varchar)) as key_no,
                    date_format(A.created_at + interval '9' hour, '%Y-%m-%d %H:%i:%s') as "제출일시",
                    max(CASE WHEN A.key in ('"cycle01_01_hello"','"cycle02_01_ready"','"cycle03_01_ready"') THEN A.value END) AS "1번",
                    max(CASE WHEN A.key in ('"cycle01_02_promise"','"cycle02_02_question"','"cycle03_02_question"') THEN A.value END) AS "2번",
                    max(CASE WHEN A.key in ('"cycle01_03_question"','"cycle02_03_compliment"','"cycle03_03_compliment"') THEN A.value END) AS "3번",
                    max(CASE WHEN A.key in ('"cycle01_04_monthlyplan"','"cycle02_04_summary"','"cycle03_04_summary"') THEN A.value END) AS "4번",
                    max(CASE WHEN A.key in ('"cycle01_05_respect"','"cycle02_05_respect"','"cycle03_05_respect"') THEN A.value END) AS "5번",
                    max(CASE WHEN A.key in ('"cyMj2Q5EbCE2gOZPuvJs"','"SbXI6fGKqJeuGagtJFqU"','"9qiFkIjQ4ztJE5vyy0rY"') THEN A.value END) AS "선생님 추천 점수",
                    max(CASE WHEN A.key in ('"V3m8LmkMKv265jK0gqGR"','"CO6txyeWj4VL8EQIrkgd"','"WuBLJuA2fRJPHxfp0W3i"') THEN A.value END) AS "중립_개선점",
                    max(CASE WHEN A.key in ('"cycle01_under_8_change_tutor"','"cycle02_under_8_change_tutor"','"cycle03_under_8_change_tutor"') THEN A.value END) AS "중립_선생님 변경 희망 여부",
                    max(CASE WHEN A.key in ('"y6SQ8lSRfrrhzusiamMo"','"dKuaT9i0aX76oV3wbr6C"','"30y5Fl6sTpwI38EVY41g"') THEN A.value END) AS "비추천_개선점",
                    max(CASE WHEN A.key in ('"cycle01_under_6_change_tutor"','"cycle02_under_6_change_tutor"','"cycle03_under_6_change_tutor"') THEN A.value END) AS "비추천_선생님 변경 희망 여부"
            from (
                    select
                            lecture_vt_no,
                            cycle_count,
                            created_at,
                            tutor_user_id,
                            student_user_id,
                            replace(replace(concat_ws('',map_keys.key),'{',''),'}','') as key,
                            replace(replace(replace(concat_ws('',map_keys.value),'{',''),'}',''),'"','') as value
                    from (
                            select *
                            from (
                                    select
                                            row_number() over(
                                                partition by sf.lecture_vt_no, sf.tutor_user_id, sf.cycle_count
                                                order by sf.created_at desc
                                            ) as rn,
                                            sf.*
                                    from marketing_scylladb.marketing_mbti.student_feedback_projects sf
                                    where sf.created_at > timestamp '2025-11-01 00:00:00'
                                      and sf.cycle_count <= 3
                            ) sf
                            where sf.rn = 1
                    ) nps_raw
                    CROSS JOIN UNNEST(split_to_multimap(nps_raw.body_list_map, '",', ':')) map_keys(key, value)
            ) A
            group by A.lecture_vt_no, A.tutor_user_id, A.cycle_count, A.created_at
    ),
    link as ( 
            select s.schedule_no,
                    concat('https://app.pagecall.net/replay/',
                        trim(cast(lvc.page_call_room_id as varchar)),
                        '?access_token=',
                        trim(cast(lt.pagecall_access_token as varchar)),
                        '&debug=1') as link,
                    lvc.durations
                    from sch_new s
                left join mysql.onuei.lecture_vt_cycles lvc on s.lecture_cycle_no = lvc.lecture_cycle_no
                left join mysql.onuei.lecture_tutor lt on lt.user_no = s.teacher_user_no
            ),
    matchingdata as (
            select g.group_lecture_vt_no, g.lecture_vt_no, md.tutor_id, md.matchedat,
                    ROW_NUMBER() OVER (PARTITION BY g.group_lecture_vt_no, md.tutor_id ORDER BY md.matchedat ASC) AS rn
                    from glvt_all g
                    left join (
                        select mlvt.lectures[1].id AS lecture_id,
                               date_add('hour', 9, mlvt.matchedat) as matchedat,
                               mlvt.matchedteacher.id as tutor_id
                                 from matching_mongodb.matching.matching_lvt mlvt
                                 where date_add('hour', 9, mlvt.matchedat) >= timestamp '2026-01-21 00:00:00'
                        ) md on g.lecture_vt_no = md.lecture_id and g.active_timestamp < md.matchedat
            ),
    ticket as (
            select * from (
                select cst.lecture_vt_no, cst.update_datetime, substr(cst.content, 34, 3) as tname,
                       row_number() over (partition by cst.lecture_vt_no, substr(cst.content, 34, 3) order by cst.update_datetime desc) as rn
                from mysql.onuei.customer_service_ticket cst 
                where cst.update_datetime > timestamp '2025-11-01 00:00:00'
                  and cst.content like ('%첫 메시지 도착%')
            ) sub
            where rn = 1
            ),
    final_base as (
            select sn.lecture_vt_no,
                    case
                        when sn.done_rank = 0 and sn.schedule_state in ('RESERVATION','CONTRACT') then '첫 수업 전'
                        when sn.done_rank = 1 and sn.schedule_state in ('DONE') then '1회차 완료'
                        when sn.done_rank = 1 and sn.schedule_state in ('RESERVATION') then '2회차 전'
                        when sn.done_rank = 2 and sn.schedule_state in ('DONE') then '2회차 완료'
                        when sn.done_rank = 2 and sn.schedule_state in ('RESERVATION') then '3회차 전'
                        when sn.done_rank = 3 and sn.schedule_state in ('DONE') then '3회차 완료'
                        when sn.done_rank = 3 and sn.schedule_state not in ('DONE') then '종료'
                        when sn.done_rank >= 4 then '종료'
                        when sn.schedule_state in ('TUTORING') then '수업 중'
                        when sn.schedule_state in ('CANCEL') then '취소'
                        else ' '
                    end as "회차",
                    sn.schedule_state, sn.teacher_user_no, t.t_name, t.lecture_phone_number, lv.name, lv.subject, lv.tutoring_state,
                    case when sn.schedule_state = 'DONE' then sn.update_datetime else null end as "마치기 시점",
                    sn.tutoring_datetime, nps."제출일시", lk.link,
                    case 
                        when date_diff('hour', md.matchedat, tk.update_datetime) >= 24 then '초과'
                        when date_diff('hour', md.matchedat, tk.update_datetime) < 24 then '통과'
                        when sn.schedule_state <> 'DONE' or sn.schedule_state is null then ''
                        else '확인 필요'
                    end as "위반",
                    sn.create_datetime,
                    nps."1번" as q1, nps."2번" as q2, nps."3번" as q3, nps."4번" as q4, nps."5번" as q5,
                    md.matchedat
                    from ltvt_new_mapped nm
                    join sch_new sn on nm.lecture_teacher_vt_no = sn.lecture_teacher_vt_no
                    left join lvts lv on sn.lecture_vt_no = lv.lecture_vt_no
                    left join t on sn.teacher_user_no = t.teacher_user_no
                    left join feedback fb on sn.schedule_no = fb.schedule_no
                    left join nps on nps.key_no = fb.key_no
                    left join link lk on lk.schedule_no = sn.schedule_no
                    left join matchingdata md on nm.group_lecture_vt_no = md.group_lecture_vt_no
                       and md.tutor_id = sn.teacher_user_no and md.rn = 1
                    left join ticket tk on sn.lecture_vt_no = tk.lecture_vt_no and tk.tname = t.t_name
                    where lv.name not like ('%테스트%')
                        and lv.phone_number not like ('%00000000%')
                        and lv.email_id not like ('%@seoltab.test%')
                        and (case when sn.schedule_state = 'DONE' then sn.update_datetime else null end) is not null
            )
select lecture_vt_no, "회차", schedule_state, teacher_user_no, t_name, lecture_phone_number, name, subject as "과목명", tutoring_state, "마치기 시점", tutoring_datetime, "제출일시", link, "위반"
from final_base
where (
    (q1 is not null and TRY_CAST(q1 as integer) = 0)
    or (q2 is not null and TRY_CAST(q2 as integer) = 0)
    or (q3 is not null and TRY_CAST(q3 as integer) = 0)
    or (q4 is not null and TRY_CAST(q4 as integer) = 0)
    or (q5 is not null and TRY_CAST(q5 as integer) = 0)
  )
  and create_datetime >= timestamp '2026-01-21 00:00:00'
  and matchedat >= timestamp '2026-01-21 00:00:00'
order by "제출일시" asc, create_datetime asc, lecture_vt_no asc
'''


operation_sheet_query = '''
-- 추출 쿼리 (0211 수정버전)
with 
    glvt_all as (
        select
            glvt.group_lecture_vt_no,
            glvt.lecture_vt_no,
            glvt.active_timestamp,
            LEAD(glvt.active_timestamp) OVER (
                PARTITION BY glvt.lecture_vt_no
                ORDER BY glvt.active_timestamp
            ) AS next_active_timestamp
        from data_warehouse.raw_data.group_lvt glvt
    ),
    lvts as (
        select
            lvt.lecture_vt_no,
            u.user_no,
            u.name,
            u.phone_number,
            u.email_id,
            s.parent_name,
            s.parent_phone_number,
            lvt.tutoring_state,
            ttn.name as subject,
            lvt.lecture_subject_id
        from mysql.onuei.lecture_video_tutoring lvt
        join mysql.onuei.user u
            on lvt.student_user_no = u.user_no
        left join mysql.onuei.student s
            on lvt.student_user_no = s.user_no
        left join mysql.onuei.term_taxonomy_name ttn
            on lvt.lecture_subject_id = ttn.term_taxonomy_id
        where lvt.student_type not in ('CTEST')
    ),
    t as (
        select
            te.user_no as teacher_user_no,
            ut.name as t_name,
            te.lecture_phone_number
        from mysql.onuei.teacher te
        left join mysql.onuei.user ut
            on te.user_no = ut.user_no
        where te.seoltab_state is not null
          and te.lecture_phone_number not in ('01000000000','01099999999')
    ),
    ltvt_new_base as (
        select
            ltvt.lecture_teacher_vt_no,
            ltvt.lecture_vt_no,
            ltvt.teacher_user_no,
            ltvt.teacher_vt_status,
            ltvt.total_done_month,
            ltvt.create_at
        from mysql.onuei.lecture_teacher_vt ltvt
    ),
    ltvt_new_mapped as (
        select
            g.group_lecture_vt_no,
            g.active_timestamp,
            g.next_active_timestamp,
            nb.lecture_teacher_vt_no,
            nb.lecture_vt_no,
            nb.teacher_user_no,
            nb.teacher_vt_status,
            nb.total_done_month,
            nb.create_at
        from ltvt_new_base nb
        join glvt_all g
            on nb.lecture_vt_no = g.lecture_vt_no
           and nb.create_at >= g.active_timestamp
           and (g.next_active_timestamp is null or nb.create_at < g.next_active_timestamp)
    ),
    sch_new as (
        select *
        from (
            select
                nm.group_lecture_vt_no,
                nm.lecture_teacher_vt_no,
                lvs.lecture_vt_no,
                lvs.schedule_no,
                lvs.create_datetime,
                lvs.tutoring_datetime,
                lvs.update_datetime,
                lvs.schedule_state,
                sf.teacher_user_no,
                sf.follow_no,
                lvs.lecture_cycle_no,
                sf.student_user_no,
                ROW_NUMBER() OVER (
                    PARTITION BY nm.lecture_teacher_vt_no
                    ORDER BY lvs.create_datetime ASC, lvs.schedule_no ASC
                ) AS schedule_rank,
                SUM(CASE WHEN lvs.schedule_state = 'DONE' THEN 1 ELSE 0 END) OVER (
                    PARTITION BY nm.lecture_teacher_vt_no
                    ORDER BY lvs.create_datetime ASC, lvs.schedule_no ASC
                ) AS done_rank
            from ltvt_new_mapped nm
            join mysql.onuei.lecture_vt_schedules lvs
                on nm.lecture_vt_no = lvs.lecture_vt_no
               and lvs.create_datetime >= nm.create_at
            join mysql.onuei.student_follow sf
                on lvs.follow_no = sf.follow_no
               and sf.teacher_user_no = nm.teacher_user_no
        ) x
        where schedule_rank <= 4
    ),
    feedback as (
        select
            concat(
                cast(lsf.lecture_vt_no as varchar), '_',
                cast(lsf.teacher_id as varchar), '_',
                cast(lsf.feedback_cycle as varchar)
            ) as key_no,
            lsf.schedule_no
        from mysql.onuei.lecture_student_feedback lsf
        where lsf.created_at >= timestamp '2025-11-01 00:00:00'
          and feedback_cycle <= 3
    ),
    nps as (
        select
            concat(
                cast(A.lecture_vt_no as varchar), '_',
                cast(A.tutor_user_id as varchar), '_',
                cast(A.cycle_count as varchar)
            ) as key_no,
            date_format(A.created_at + interval '9' hour, '%Y-%m-%d %H:%i:%s') as "제출일시",
            max(CASE WHEN A.key in ('"cycle01_01_hello"','"cycle02_01_ready"','"cycle03_01_ready"') THEN A.value END) AS "1번",
            max(CASE WHEN A.key in ('"cycle01_02_promise"','"cycle02_02_question"','"cycle03_02_question"') THEN A.value END) AS "2번",
            max(CASE WHEN A.key in ('"cycle01_03_question"','"cycle02_03_compliment"','"cycle03_03_compliment"') THEN A.value END) AS "3번",
            max(CASE WHEN A.key in ('"cycle01_04_monthlyplan"','"cycle02_04_summary"','"cycle03_04_summary"') THEN A.value END) AS "4번",
            max(CASE WHEN A.key in ('"cycle01_05_respect"','"cycle02_05_respect"','"cycle03_05_respect"') THEN A.value END) AS "5번",
            max(CASE WHEN A.key in ('"cyMj2Q5EbCE2gOZPuvJs"','"SbXI6fGKqJeuGagtJFqU"','"9qiFkIjQ4ztJE5vyy0rY"') THEN A.value END) AS "선생님 추천 점수",
            max(CASE WHEN A.key in ('"V3m8LmkMKv265jK0gqGR"','"CO6txyeWj4VL8EQIrkgd"','"WuBLJuA2fRJPHxfp0W3i"') THEN A.value END) AS "중립_개선점",
            max(CASE WHEN A.key in ('"cycle01_under_8_change_tutor"','"cycle02_under_8_change_tutor"','"cycle03_under_8_change_tutor"') THEN A.value END) AS "중립_선생님 변경 희망 여부",
            max(CASE WHEN A.key in ('"y6SQ8lSRfrrhzusiamMo"','"dKuaT9i0aX76oV3wbr6C"','"30y5Fl6sTpwI38EVY41g"') THEN A.value END) AS "비추천_개선점",
            max(CASE WHEN A.key in ('"cycle01_under_6_change_tutor"','"cycle02_under_6_change_tutor"','"cycle03_under_6_change_tutor"') THEN A.value END) AS "비추천_선생님 변경 희망 여부"
        from (
            select
                lecture_vt_no,
                cycle_count,
                created_at,
                tutor_user_id,
                student_user_id,
                replace(replace(concat_ws('', map_keys.key), '{',''),'}','') as key,
                replace(replace(replace(concat_ws('', map_keys.value), '{',''),'}',''),'"','') as value
            from (
                select *
                from (
                    select
                        row_number() over(
                            partition by sf.lecture_vt_no, sf.tutor_user_id, sf.cycle_count
                            order by sf.created_at desc
                        ) as rn,
                        sf.*
                    from marketing_scylladb.marketing_mbti.student_feedback_projects sf
                    where sf.created_at > timestamp '2025-11-01 00:00:00'
                      and sf.cycle_count <= 3
                ) sf
                where sf.rn = 1
            ) nps_raw
            cross join unnest(split_to_multimap(nps_raw.body_list_map, '",', ':')) map_keys(key, value)
        ) A
        group by A.lecture_vt_no, A.tutor_user_id, A.cycle_count, A.created_at
    ),
    matchingdata as (
        select
            g.group_lecture_vt_no,
            g.lecture_vt_no,
            md.tutor_id,
            md.matchedat,
            ROW_NUMBER() OVER (
                PARTITION BY g.group_lecture_vt_no, md.tutor_id
                ORDER BY md.matchedat ASC
            ) AS rn
        from glvt_all g
        left join (
            select
                mlvt.lectures[1].id AS lecture_id,
                date_add('hour', 9, mlvt.matchedat) as matchedat,
                mlvt.matchedteacher.id as tutor_id
            from matching_mongodb.matching.matching_lvt mlvt
            where date_add('hour', 9, mlvt.matchedat) >= timestamp '2026-01-21 00:00:00'
        ) md
            on g.lecture_vt_no = md.lecture_id
           and g.active_timestamp < md.matchedat
    ),
    final_base as (
        select
            sn.lecture_vt_no,
            case
                when sn.done_rank = 0 and sn.schedule_state in ('RESERVATION','CONTRACT') then '첫 수업 전'
                when sn.done_rank = 1 and sn.schedule_state in ('DONE') then '1회차 완료'
                when sn.done_rank = 1 and sn.schedule_state in ('RESERVATION') then '2회차 전'
                when sn.done_rank = 2 and sn.schedule_state in ('DONE') then '2회차 완료'
                when sn.done_rank = 2 and sn.schedule_state in ('RESERVATION') then '3회차 전'
                when sn.done_rank = 3 and sn.schedule_state in ('DONE') then '3회차 완료'
                when sn.done_rank = 3 and sn.schedule_state not in ('DONE') then '종료'
                when sn.done_rank >= 4 then '종료'
                when sn.schedule_state in ('TUTORING') then '수업 중'
                when sn.schedule_state in ('CANCEL') then '취소'
                else ' '
            end as "회차",
            case when sn.schedule_state = 'DONE' then sn.update_datetime else null end as "마치기 시점",
            nps."제출일시",
            sn.tutoring_datetime,
            lv.user_no,
            lv.name,
            lv.phone_number,
            t.t_name,
            lv.parent_name,
            lv.parent_phone_number,
            lv.subject,
            try_cast(nps."선생님 추천 점수" as integer) as score_num,
            case
                when nps."중립_선생님 변경 희망 여부" in ('yes','yes_timechange') then nps."중립_선생님 변경 희망 여부"
                when nps."비추천_선생님 변경 희망 여부" in ('yes','yes_timechange') then nps."비추천_선생님 변경 희망 여부"
                else 'X'
            end as "변경희망여부",
            sn.create_datetime,
            nps."1번" as q1,
            nps."2번" as q2,
            nps."3번" as q3,
            nps."4번" as q4,
            nps."5번" as q5,
            md.matchedat
        from ltvt_new_mapped nm
        join sch_new sn
            on nm.lecture_teacher_vt_no = sn.lecture_teacher_vt_no
        left join lvts lv
            on sn.lecture_vt_no = lv.lecture_vt_no
        left join t
            on sn.teacher_user_no = t.teacher_user_no
        left join feedback fb
            on sn.schedule_no = fb.schedule_no
        left join nps
            on nps.key_no = fb.key_no
        left join matchingdata md
            on nm.group_lecture_vt_no = md.group_lecture_vt_no
           and md.tutor_id = sn.teacher_user_no
           and md.rn = 1
        where lv.name not like ('%테스트%')
          and lv.phone_number not like ('%00000000%')
          and lv.email_id not like ('%@seoltab.test%')
          and (case when sn.schedule_state = 'DONE' then sn.update_datetime else null end) is not null
          and md.matchedat >= timestamp '2026-01-21 00:00:00'
          and sn.create_datetime >= timestamp '2026-01-21 00:00:00'
          and nm.create_at >= timestamp '2026-01-21 00:00:00'
    )
select
    lecture_vt_no,
    "회차",
    "마치기 시점",
    "제출일시",
    tutoring_datetime,
    user_no,
    name,
    phone_number,
    t_name,
    parent_name,
    parent_phone_number,
    subject,
    score_num as "선생님 추천 점수",
    "변경희망여부" as "변경 희망 여부"
from final_base
where (
    score_num <= 7
    or (
        (q1 is not null and try_cast(q1 as integer) = 0)
        or (q2 is not null and try_cast(q2 as integer) = 0)
        or (q3 is not null and try_cast(q3 as integer) = 0)
        or (q4 is not null and try_cast(q4 as integer) = 0)
        or (q5 is not null and try_cast(q5 as integer) = 0)
    )
    or "변경희망여부" in ('yes','yes_timechange')
)
order by "제출일시" asc, create_datetime asc, lecture_vt_no asc
'''


alimtalk_sheet_query = '''
-- 책임 수업 & 매칭 제도 대상자 추출
with 
    glvt as (
            select glvt.group_lecture_vt_no,glvt.lecture_vt_no , glvt.active_timestamp , glvt.done_month ,glvt.done_timestamp,
                    LEAD(active_timestamp) OVER (PARTITION BY lecture_vt_no ORDER BY active_timestamp) AS next_active_timestamp
                    from data_warehouse.raw_data.group_lvt glvt
                    where glvt.active_timestamp >= timestamp '2025-11-01 00:00:00'
                    and glvt.active_timestamp < timestamp '2026-01-21 00:00:00'
                ),
    lvts as (
            select lvt.lecture_vt_no ,lvt.student_type, u.user_no, u.name,u.phone_number,u.email_id,s.parent_name ,s.parent_phone_number ,lvt.payment_item , lvt.application_datetime, lvt.tutoring_state, ttn.name as "subject",lvt.lecture_subject_id, lvt.total_subject_done_month
            from mysql.onuei.lecture_video_tutoring lvt
            inner join 
                    (select u.user_no, u.name, u.phone_number, u.email_id
                            from mysql.onuei.user u) u on lvt.student_user_no = u.user_no
            left join mysql.onuei.student s on lvt.student_user_no = s.user_no 
            left join mysql.onuei.term_taxonomy_name ttn on lvt.lecture_subject_id = ttn.term_taxonomy_id 
            where lvt.student_type not in ('CTEST')
          ),
    t as (
            select t.user_no ,t.seoltab_state ,t.seoltab_state_updateat ,ut.name as t_name, t.lecture_phone_number 
                    from mysql.onuei.teacher t
                    left join mysql.onuei.user ut on t.user_no = ut.user_no 
                    where t.seoltab_state is not null
                    and t.lecture_phone_number not in ('01000000000')
                    and t.lecture_phone_number not in ('01099999999')
            ),
         sch AS (
            SELECT *
            FROM (
                select
                    glvt.group_lecture_vt_no,
                    lvs.lecture_vt_no,
                    lvs.schedule_no,
                    lvs.create_datetime,
                    lvs.tutoring_datetime,
                    lvs.update_datetime,
                    lvs.schedule_state,
                    sf.teacher_user_no,
                    sf.follow_no,
                    lvs.lecture_cycle_no,
                    sf.student_user_no,
                    ROW_NUMBER() OVER (PARTITION BY lvs.lecture_vt_no ORDER BY lvs.create_datetime ASC) AS schedule_rank,
                    SUM(CASE WHEN lvs.schedule_state = 'DONE' THEN 1 ELSE 0 END)
                        OVER (PARTITION BY lvs.lecture_vt_no ORDER BY lvs.create_datetime ASC) AS done_rank
                FROM glvt
                left join mysql.onuei.lecture_vt_schedules lvs on glvt.lecture_vt_no = lvs.lecture_vt_no 
                    and glvt.active_timestamp <= lvs.create_datetime 
                    AND (glvt.next_active_timestamp IS NULL OR lvs.create_datetime < glvt.next_active_timestamp)
                INNER JOIN mysql.onuei.student_follow sf ON lvs.follow_no = sf.follow_no
            ) sub
            WHERE schedule_rank <= 4
        ),
        feedback as (
                        select concat(cast(lsf.lecture_vt_no as varchar),'_',cast(lsf.teacher_id as varchar),'_',cast(lsf.feedback_cycle as varchar))as key_no, lsf.lecture_vt_no, lsf.student_id , lsf.teacher_id ,lsf.feedback_cycle , lsf.schedule_no , lsf.created_at 
                                from mysql.onuei.lecture_student_feedback lsf 
                                where lsf.created_at >= timestamp '2025-11-01 00:00:00'
                                and feedback_cycle <= 3
                        ),
        nps as (
                select concat(cast(A.lecture_vt_no as varchar),'_',cast(A.tutor_user_id as varchar),'_',cast(A.cycle_count as varchar))as key_no,
                        A.rn, A.lecture_vt_no,A.cycle_count,A.tutor_user_id,date_format(A.created_at + interval '9' hour, '%Y-%m-%d %H:%i:%s') as "제출일시",A.student_user_id
                        ,max(CASE WHEN A.key in ('"cycle01_01_hello"','"cycle02_01_ready"','"cycle03_01_ready"') THEN A.value END) AS "1번"
                        ,max(CASE WHEN A.key in ('"cycle01_02_promise"','"cycle02_02_question"','"cycle03_02_question"') THEN A.value END) AS "2번"
                        ,max(CASE WHEN A.key in ('"cycle01_03_question"','"cycle02_03_compliment"','"cycle03_03_compliment"') THEN A.value END) AS "3번"
                        ,max(CASE WHEN A.key in ('"cycle01_04_monthlyplan"','"cycle02_04_summary"','"cycle03_04_summary"') THEN A.value END) AS "4번"
                        ,max(CASE WHEN A.key in ('"cycle01_05_respect"','"cycle02_05_respect"','"cycle03_05_respect"') THEN A.value END) AS "5번"
                        ,max(CASE WHEN A.key in ('"cyMj2Q5EbCE2gOZPuvJs"','"SbXI6fGKqJeuGagtJFqU"','"9qiFkIjQ4ztJE5vyy0rY"') THEN A.value END) AS "선생님 추천 점수"
                        ,max(CASE WHEN A.key in ('"V3m8LmkMKv265jK0gqGR"','"CO6txyeWj4VL8EQIrkgd"','"WuBLJuA2fRJPHxfp0W3i"') THEN A.value END) AS "중립_개선점"
                        ,max(CASE WHEN A.key in ('"cycle01_under_8_change_tutor"','"cycle02_under_8_change_tutor"','"cycle03_under_8_change_tutor"') THEN A.value END) AS "중립_선생님 변경 희망 여부"
                        ,max(CASE WHEN A.key in ('"y6SQ8lSRfrrhzusiamMo"','"dKuaT9i0aX76oV3wbr6C"','"30y5Fl6sTpwI38EVY41g"') THEN A.value END) AS "비추천_개선점"
                        ,max(CASE WHEN A.key in ('"cycle01_under_6_change_tutor"','"cycle02_under_6_change_tutor"','"cycle03_under_6_change_tutor"') THEN A.value END) AS "비추천_선생님 변경 희망 여부"
                        from (
                                select rn,lecture_vt_no,cycle_count,created_at,tutor_user_id,student_user_id 
                                                ,replace(replace(concat_ws('',map_keys.key),'{',''),'}','') as key,replace(replace(replace(concat_ws('',map_keys.value),'{',''),'}',''),'"','') as value
                                        from (
                                            select *
                                                    from
                                                    (select row_number() over(partition by sf.lecture_vt_no order by sf.created_at asc) as rn,sf.*
                                                            from marketing_scylladb.marketing_mbti.student_feedback_projects sf
                                                            where sf.created_at > timestamp '2025-11-01 00:00:00') sf
                                                    where sf.rn <= 3
                                                    ) nps_raw
                                    CROSS JOIN UNNEST(split_to_multimap(nps_raw.body_list_map, '",', ':')) map_keys(key, value)
                                ) A
                group by A.rn,A.lecture_vt_no,A.cycle_count,A.created_at,A.tutor_user_id,student_user_id 
                ),
            link as ( 
                select sch.schedule_no ,sch.lecture_vt_no, sch.schedule_state, sch.tutoring_datetime , sch.student_user_no,u.name as sname, sch.teacher_user_no,ut.name as tname, lvc.page_call_room_id , lt.pagecall_access_token, lvc.durations
                                ,concat(
                                    'https://app.pagecall.net/replay/',
                                    lvc.page_call_room_id,
                                    '?access_token=',
                                    trim(cast(lt.pagecall_access_token as varchar)),
                                    '&debug=1'
                                ) as link 
                        from sch
                    left join (
                                                  select lecture_cycle_no, 
                                                         rtrim(cast(page_call_room_id as varchar)) as page_call_room_id,
                                                         durations
                                                  from mysql.onuei.lecture_vt_cycles
                                                ) lvc on sch.lecture_cycle_no = lvc.lecture_cycle_no
                        left join mysql.onuei.lecture_tutor lt on lt.user_no = sch.teacher_user_no
                        left join mysql.onuei.user u on sch.student_user_no = u.user_no 
                        left join mysql.onuei.user ut on sch.teacher_user_no  = ut.user_no 
                ),
    matchingdata as (
            select glvt.group_lecture_vt_no, glvt.active_timestamp, glvt.lecture_vt_no
                                    ,row_number () over (partition by glvt.group_lecture_vt_no, md.tutor_id order by md.matchedat asc) as rn, md.*, t.t_name
                                    ,CASE 
                                        WHEN md.matchedat >= CAST('2026-01-21 00:00:00' AS timestamp) THEN 'EXCLUDE'
                                        ELSE NULL
                                     END AS date_filter_flag
                    from glvt                
                        left join
                        (SELECT
                          mlvt.lectures[1].id AS lecture_id,
                          mlvt.lectures[1].student.id AS student_id,
                        mlvt.status,mlvt.teachersuggestionstatus,
                        date_add('hour', 9, mlvt.matchedat) as matchedat,
                        mlvt.updatedat,
                        mlvt.matchedteacher.id as tutor_id
                                 from matching_mongodb.matching.matching_lvt mlvt
                                 where DATE_ADD('hour', 9, mlvt.matchedat) >= cast ('2025-11-01 00:00:00' as timestamp)
                            )md on glvt.lecture_vt_no = md.lecture_id and glvt.active_timestamp < md.matchedat
                        left join t on md.tutor_id = t.user_no
                        ),
        ltvt as (
                        select glvt.group_lecture_vt_no , glvt.lecture_vt_no , glvt.active_timestamp ,ltvt.teacher_user_no , ltvt.teacher_vt_status , ltvt.total_done_month, ltvt.create_at , ltvt.reactive_at 
                                from glvt
                                left join mysql.onuei.lecture_teacher_vt ltvt on glvt.lecture_vt_no = ltvt.lecture_vt_no AND (glvt.next_active_timestamp IS NULL OR ltvt.reactive_at < glvt.next_active_timestamp)
                        ),
    ticket as (
                    select * from (
                        select 
                            cst.lecture_vt_no, 
                            cst.content, 
                            cst.update_datetime, 
                            substr(cst.content, 34, 3) as tname,
                            row_number() over (partition by cst.lecture_vt_no, substr(cst.content, 34, 3) 
                                              order by cst.update_datetime desc) as rn
                        from mysql.onuei.customer_service_ticket cst 
                        where cst.update_datetime > timestamp '2025-11-01 00:00:00'
                          and cst.content like ('%첫 메시지 도착%')
                    ) sub
                    where rn = 1
                    )
select sch.lecture_vt_no,
        glvt.name as "학생 이름",
        t.t_name as "선생님 이름",
        glvt.subject as "과목명",
        case
            when sch.done_rank = 1 and sch.schedule_state = 'DONE' then '1'
            when sch.done_rank = 2 and sch.schedule_state = 'DONE' then '2'
            when sch.done_rank = 3 and sch.schedule_state = 'DONE' then '3'
        end as "회차",
        glvt.phone_number as "학생 전화번호"
from (select glvt.group_lecture_vt_no , glvt.active_timestamp , glvt.done_month , lvts.*
                            from glvt
                        left join lvts on glvt.lecture_vt_no = lvts.lecture_vt_no
                        where lvts.name not like ('%테스트%')
                                and lvts.phone_number not like ('%00000000%')
                                and lvts.email_id not like ('%@seoltab.test%')) glvt
        left join sch on glvt.group_lecture_vt_no = sch.group_lecture_vt_no 
        left join t on sch.teacher_user_no = t.user_no 
        left join feedback fb on sch.schedule_no = fb.schedule_no 
        left join nps on nps.key_no = fb.key_no  
        left join link on link.schedule_no = sch.schedule_no
        left join matchingdata md on glvt.group_lecture_vt_no = md.group_lecture_vt_no and md.tutor_id = sch.teacher_user_no and md.rn = 1
        left join ltvt on ltvt.group_lecture_vt_no = glvt.group_lecture_vt_no and ltvt.teacher_user_no = t.user_no
        left join ticket on glvt.lecture_vt_no = ticket.lecture_vt_no and ticket.tname = t.t_name
        where (md.date_filter_flag IS NULL OR md.date_filter_flag != 'EXCLUDE')
        and sch.schedule_state = 'DONE'
        and sch.update_datetime is not null
        and date(sch.update_datetime) < current_date
        and sch.update_datetime >= timestamp '2026-01-28 00:00:00'
        and nps."제출일시" is null
        and sch.done_rank in (1, 2, 3)
        and glvt.tutoring_state in ('ACTIVE', 'MATCHED')
        order by sch.update_datetime asc, sch.create_datetime asc, sch.lecture_vt_no asc
'''

