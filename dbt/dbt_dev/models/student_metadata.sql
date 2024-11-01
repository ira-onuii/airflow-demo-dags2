{{ config(
    materialized='ephemeral'
) }}


WITH user_list AS (
    select u.user_no, u.phone_number, u."name", u.sex, s.parent_name, s.parent_phone_number, u.email_id
	, las.학교명, las.희망전공, las.학년, las.지역, las.matching_condition, u.user_status, las.주소, u.join_datetime
	, p.fst_paydate, las.submitted_at, u.update_datetime, lvs.tutoring_datetime, lvt.total_count, lvt.active_count, lvt.last_done_datetime, p.total_amount
	from raw_data."user" u 
	left join 
		(select *
			from 
				(select row_number() over(partition by las.학생번호 order by las.submitted_at desc) as rn, las.submitted_at
					, las.학생번호, las.학교명, las.희망전공, las.학년, las.지역, concat(las.희망성별, chr(10), 신청_동기, chr(10), 다른학교튜터가능) as matching_condition, las."신청 ipad 색상", las.주소
					from raw_data.lecture_application_students las 
				) A
			where A.rn = 1
		) las on u.user_no = las.학생번호 
	left join 
		(select *		
			from
				(select row_number() over(partition by lvt.student_user_no order by lvs.tutoring_datetime asc) as rn
					, lvt.student_user_no, lvs.tutoring_datetime
					from raw_data.lecture_vt_schedules lvs
					inner join raw_data.lecture_video_tutoring lvt on lvs.lecture_vt_no = lvt.lecture_vt_no 
					where lvs.schedule_state <> 'CANCEL'
				) lvs
			where lvs.rn = 1
		) lvs on u.user_no = lvs.student_user_No
	left join 
		(select min(p.payment_regdate) as fst_paydate, sum(p.LGD_AMOUNT) as total_amount, p.user_No
			from raw_data.payment p
			where p.state = '결제완료'
			group by p.user_No
		) p on u.user_no = p.user_No
	left join 
		(select lvt2.student_user_No, count(*)total_count, count(case when lvt2.tutoring_state not in ('FINISH','AUTO_FINISH','DONE') then 1 else null end) as active_count
			, max(lvt2.last_done_datetime) as last_done_datetime
			from raw_data.lecture_video_tutoring lvt2
			where lvt2.student_type in ('PAYED','PAYED_B')
			group by lvt2.student_user_No
		) lvt on u.user_no = lvt.student_user_no 
	inner join raw_data.student s on u.user_no = s.user_no 
 )
SELECT *
    FROM user_list