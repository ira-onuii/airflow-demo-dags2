from datetime import datetime, timedelta

date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

table_name = f'"{date}"'


fst_lecture_query = '''
with mlvt as (
	select mlvt.lecture_vt_No
		, mlvt.teacher_user_No, mlvt.teacher_name, mlvt.matchedat, student_name
		from 
			(select row_number() over(partition by lectures[1][1] order by mlvt.matchedat asc) as rn, matchedat
				, mlvt.lectures[1][1] as lecture_vt_No
				, mlvt.matchedteacher[1] as teacher_user_No, mlvt.matchedteacher[2] as teacher_name, mlvt.lectures[1][2][2] as student_name
				from matching_mongodb.matching.matching_lvt mlvt
				where mlvt.status = 'MATCHED'
			) mlvt
		where mlvt.rn = 1
		),
	p as (
		select p.lecture_vt_no, p.payment_regdate, p.order_id, p.state 
			from 
				(select row_number() over(partition by p.lecture_vt_no order by p.payment_regdate asc) as rn 
					, p.lecture_vt_no, p.payment_regdate, p.order_id, p.state 
					from mysql.onuei.payment p
				) p
			where p.rn = 1
			and p.payment_regdate >= cast('2024-11-01 00:00:00' as timestamp)
		),
	t as (
		select t.user_No, t.seoltab_state_updateat, t.lecture_phone_number 
			from mysql.onuei.teacher t
			where t.seoltab_state = 'ACTIVE'
			and t.seoltab_state_updateat >= cast('2024-11-01 00:00:00' as timestamp)
		),
	meta_data as (
		select lvt.student_user_No ,lvt.lecture_vt_no, ttn.name as subject, th.tteok_ham_type, u.phone_number, lvt.tutoring_state, u.name as student_name
			, lvt.reactive_datetime 
			from mysql.onuei.lecture_video_tutoring lvt
			inner join mysql.onuei."user" u on lvt.student_user_no = u.user_no 
			inner join mysql.onuei.tteok_ham th on lvt.payment_item = th.tteok_ham_no 
			inner join mysql.onuei.term_taxonomy_name ttn on lvt.lecture_subject_id = ttn.term_taxonomy_id 
			-- where lvt.reactive_datetime is null
			-- and lvt.tutoring_state = 'ACTIVE'
	)
select mlvt.lecture_vt_No, md.subject, md.student_user_No, mlvt.student_name
	, mlvt.teacher_user_No, mlvt.teacher_name, lvc.rn, lvc.page_call_room_id, lvc.tutoring_datetime
	from mlvt
	left join
		(select row_number() over(partition by lvs.lecture_vt_no order by lvs.tutoring_datetime asc) as rn
			, lvs.lecture_vt_no, sf.teacher_user_no, lvc.page_call_room_id, lvs.tutoring_datetime 
			from mysql.onuei.lecture_vt_schedules lvs
			inner join mysql.onuei.student_follow sf on lvs.follow_no = sf.follow_no 
			inner join mysql.onuei.lecture_vt_cycles lvc on lvs.lecture_cycle_no = lvc.lecture_cycle_no 
			where lvc.req_datetime >= cast('2024-11-01 00:00:00' as timestamp)
            and lvs.tutoring_datetime >= cast('2024-11-15 00:00:00' as timestamp)
		) lvc on (mlvt.lecture_vt_No = lvc.lecture_vt_no and mlvt.teacher_user_No = lvc.teacher_user_No)
	inner join p on mlvt.lecture_vt_No = p.lecture_vt_no 
	left join meta_data md on md.lecture_vt_No = mlvt.lecture_vt_No
	inner join t on mlvt.teacher_user_No = t.user_no 
	inner join mysql.onuei.lecture_teacher_vt ltvt on (mlvt.lecture_vt_No = ltvt.lecture_vt_no and mlvt.teacher_user_No = ltvt.teacher_user_no)
	-- where ltvt.teacher_vt_status = 'ASSIGN'
	order by mlvt.lecture_vt_No, lvc.rn
'''