{{ config(
    materialized='table'
) }}


select row_number() over(order by lt.create_datetime asc) as id, t.user_no as teacher_id, u.phone_number, u."name", u.sex as gender, u.email_id as email
	, su.schoolname as school, t.teacher_school_subject as major, (extract(year from now())-u.birth_year)+1 as "age", t.hakbun
	, t.univ_graduate_type as university_state, s.schooltype as graduate_highschool_type, s.schoolname as highschool_name, s.region as highschool_region
	, stc.tutoring_pr as introductuoin, t.seoltab_tutoring_on_off as matching_abailable,t.selected_subjects as abailable_subject, '' as status
	, lt.rental_fee_type as device_state, '' as penalty, u.join_datetime as joined_at, lt.create_datetime as submitted_at
	, t.seoltab_state_updateat as examined_at, t.seoltab_state_updateat as signed_at, t.seoltab_state_updateat as active_at, t.seoltab_state_updateat as done_at
	, m.first_suggeste_at, m.last_suggeste_at, m.suggestion_count, m.accept_count, m.refugee_count
	, cancel.total_lecture_count, cancel.changed_schedule_count, last_round_done_at
	, a.point as total_point, a.cash as cashout_amount
	from raw_data.teacher t 
	inner join raw_data."user" u on t.user_no = u.user_no 
	inner join raw_data.lecture_tutor lt on t.user_no = lt.user_no 
	inner join raw_data.school_university su on u.school_seq = su.seq 
	inner join raw_data.school s on t.graduate_highschool_seq = s.seq 
	inner join raw_data.seoltab_teacher_config stc on t.user_no = stc.user_no 
	inner join 
		(select suggestion_teacher_id
			, min(m.suggestedat) as first_suggeste_at, max(m.suggestedat) as last_suggeste_at
			, count(*) as suggestion_count
			, count(case when m.ms_status = 'MATCHED' then 1 else null end) as accept_count
			, count(case when m.ms_status = 'REFUSED'then 1 else null end) as refugee_count
			from raw_data.matching m
			group by m.suggestion_teacher_id
		) m on t.user_no = m.suggestion_teacher_id
	left join 
		(select ltvt.teacher_user_no, sum(lvs.changed_schedule_count) as changed_schedule_count
			, count(*) as total_lecture_count
			, count(case when lvs.ct = 0 and ltvt.teacher_vt_status = 'UNASSIGN' then 1 else null end) as before_first_round_done_cancel_count
			, count(case when ltvt.teacher_vt_status = 'ASSIGN' then 1 else null end) as active_lecture_count
			, max(last_done_at) as last_round_done_at
			from raw_data.lecture_teacher_vt ltvt
			left join
				(select lvs.lecture_vt_no, sf.teacher_user_No, count(case when lvs.schedule_state = 'DONE' then 1 else null end) as ct
					, sum(lvtsh.ct) as changed_schedule_count
					, max(case when lvs.schedule_state = 'DONE' then lvs.update_datetime else null end) as last_done_at
					from raw_data.lecture_vt_schedules lvs 
					inner join raw_data.student_follow sf on lvs.follow_no = sf.follow_No
					left join 
						(select lvtsh."schedule_No", count(*) as ct
							from raw_data.lecture_vt_schedules_history lvtsh
							where lvtsh.change_type = 'TEACHER'
							group by lvtsh."schedule_No"	
						) lvtsh on lvs.schedule_No = lvtsh."schedule_No"
					-- where lvs.schedule_state = 'DONE'
					group by lvs.lecture_vt_no, sf.teacher_user_No
				) lvs on (ltvt.teacher_user_no = lvs.teacher_user_No and ltvt.lecture_vt_no = lvs.lecture_vt_No)
			group by ltvt.teacher_user_no 
		) cancel on t.user_no = cancel.teacher_user_No
	left join 
		(select a.user_No, sum(cash) as cash, sum(point) as point
			from raw_data.account a
			group by a.user_No
		) a on t.user_no = a.user_No