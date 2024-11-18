{{ config(
    materialized='ephemeral'
) }}


WITH join_first_payment_list AS (
    select lvt.student_user_No,lvt.tutoring_state,lvt.reactive_datetime,lvt.subject,lvt.name, lvt.phone_number
		,dense_rank() over(partition by lvt.student_user_No order by p.fst_order_id asc) as rn
		from  
			(select lvt.lecture_vt_No, u.name,lvt.student_user_no, lvt.tutoring_state,lvt.reactive_datetime,ttn.name as subject, u.phone_number
				from mysql.onuei.lecture_video_tutoring lvt
				inner join mysql.onuei."user" u on lvt.student_user_No = u.user_no 
				inner join mysql.onuei.term_taxonomy_name ttn on lvt.lecture_subject_id = ttn.term_taxonomy_id 
				inner join mysql.onuei.tteok_ham th on lvt.payment_item = th.tteok_ham_no 
				inner join mysql.onuei.student s on lvt.student_user_No = s.user_no 
				inner join mysql.onuei.term_taxonomy_name ttn2 on s."year" = ttn2.term_taxonomy_id 
				where u.email_id not like '%onuii%'
				and u.email_id not like '%test%'
				and u.phone_number not like '%00000%'
				and u.phone_number not like '%11111%'
				and u.name not like '%테스트%'
	            and u.user_No not in (621888,621889,622732,615701)
				and lvt.student_type in ('PAYED','PAYED_B','CANCEL')
	--				and lvt.student_user_No in
	--					(select student_user_No
	--						from mysql.onuei.lecture_video_tutoring 
	--						where tutoring_state not in ('FINISH','AUTO_FINISH','DONE'))
				and th.tteok_ham_type <> 'CONTENTS'
			) lvt
		left join 
			(select p.lecture_vt_No,min(p.order_id) as fst_order_id
				from mysql.onuei.payment p
				inner join mysql.onuei.tteok_ham th on p.tteok_ham_No = th.tteok_ham_no 
				where (p.state is null or p.state = '결제완료')
				and th.item_detailed_type = '수업료'
				group by p.lecture_vt_No
			) p on lvt.lecture_vt_No = p.lecture_vt_No
)
SELECT *
    FROM join_first_payment_list jfpl
    where jfpl.rn = 1
