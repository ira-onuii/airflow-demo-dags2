from datetime import datetime, timedelta

date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

table_name = f'"{date}"'



las_select_query = '''
select lecture_vt_no, `학생번호`, `학교명`, `희망전공`, `학년`, `주소`, `지역`, `희망성별`, `신청_동기`, `다른학교튜터가능`, `신청 ipad 색상`
	from lecture_application_students las
'''

lvt_select_query = '''
select lecture_vt_no,student_user_no,lecture_subject_id,student_type,tutoring_state,payment_item,next_payment_item,current_schedule_no,stage_max_cycle_count,stage_free_cycle_count,stage_pre_offer_cycle_count,stage_offer_cycle_count,create_datetime,update_datetime,last_done_datetime,application_datetime,memo,total_subject_done_month,reactive_datetime
	from lecture_video_tutoring lvt
'''

user_select_query = '''
select user_No, term_user_type, user_status, email_id, nickname, name, phone_number, device, school_seq, sex, birth_year, recent_login_datetime, join_datetime, login_version, login_device
	from user u
    where u.user_No >= 450000
'''

ttn_select_query = '''
select name, term_taxonomy_id, taxonomy, term_id, parent
	from term_taxonomy_name
'''

th_select_query = '''
select tteok_ham_No, item_type, item_detailed_type, tteok_ham_type, group_No, shelf_life, months, tteok_ham_title, tteok_ham_subtitle1, tteok_ham_subtitle2, tteok_ham_subtitle3, subjects, isshow, tteok_ham_price
	from TTEOK_HAM
'''

student_select_query = '''
select user_No, term_grade_id, parent_phone_number, parent_name, year
	from student
'''

scs_select_query = '''
select change_subject_No, lecture_change_form_No, payed_check_list, is_same_teacher, application_datetime, remark_No, remark, selected_subject, change_option_value, month_option, create_datetime, update_datetime
	from student_change_subject_v2
'''

lcf_select_query = '''
select lecture_change_form_No, lecture_vt_No, integration_No, form_type, process_status, process_failed_reason, create_datetime, update_datetime
	from lecture_change_form
'''



lvts_select_query = '''
select schedule_No, follow_No, lecture_vt_No, lecture_cycle_No, stage_count, cycle_count, is_free, offer_type, schedule_state, tutoring_datetime, last_tutoring_datetime, create_datetime, update_datetime, cycle_payment_item, per_done_month
	from lecture_VT_schedules
	where schedule_No between 20001 and 250000
'''



ltvt_select_query = '''
select lecture_teacher_vt_No, teacher_user_No, lecture_vt_No, last_schedule_No, teacher_vt_status
	, null as academic_departments, null as teacher_academic_major, null as division_of_matching_standard
    , active_done_month, total_done_month, reactive_at, create_at, update_at, lecture_subject_id
	from lecture_teacher_VT
'''

payment_select_query = '''
select payment_No, user_No, LGD_AMOUNT, LGD_BUYER, order_id, LGD_TID, LGD_PAYTYPE, TTEOK_HAM_No, LGD_PRODUCTINFO, payment_regdate, memo, lecture_vt_No, supply_value, additional_tax, cancelled_supply_value, cancelled_additional_tax, completed_at, discounted_value, cancelled_pg_tid, cancelled_amount, cancelled_pg_fee, state, original_payment_id, payment_method
	from payment p
'''

teacher_select_query = '''
select user_No, teacher_school_subject, hakbun, univ_graduate_type, graduate_highschool_seq, seoltab_tutoring_ON_OFF, selected_subjects, seoltab_state, seoltab_state_updateAT
        from teacher
'''

school_select_query = '''
select * from school
'''

stc_select_query = '''
select user_No, TUTORING_PR from seoltab_teacher_config
'''

lt_select_query = '''
select user_No, rental_fee_type, create_datetime, active_count from lecture_tutor
'''

account_select_query = '''
select user_No, cash, point from account
'''

scph_select_query = '''
select change_pause_history_No,lecture_change_form_No,lecture_vt_No
	,active_count,total_done_month,rent_total_done_month,own_total_done_month
    ,device_total_done_month,rent_type,option1,used_type
    ,process_status,create_datetime,student_type,tutoring_state
    ,name,user_No,subject,teacher_name,teacher_user_No,item_type,TTEOK_HAM_TYPE
    ,reason,reason_detail,teacher_review_content,update_datetime
    ,phone_number,parent_phone_number,teacher_phone_number,form_count,stage_offer_cycle_count,stage_pre_offer_cycle_count
	from student_change_pause_history
'''

school_university_select_query = '''
select *
	from school_university
'''



lvt_insert_query = f'''
	INSERT INTO raw_data.lecture_video_tutoring (lecture_vt_no,student_user_no,lecture_subject_id,student_type,tutoring_state,payment_item,next_payment_item,current_schedule_no,stage_max_cycle_count,stage_free_cycle_count,stage_pre_offer_cycle_count,stage_offer_cycle_count,create_datetime,update_datetime,last_done_datetime,application_datetime,memo,total_subject_done_month,reactive_datetime)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''


user_insert_query = f'''
	INSERT INTO raw_data."user" (user_No, term_user_type, user_status, email_id, nickname, name, phone_number, device, school_seq, sex, birth_year, recent_login_datetime, join_datetime, login_version, login_device)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

ttn_insert_query = f'''
	INSERT INTO raw_data.term_taxonomy_name (name, term_taxonomy_id, taxonomy, term_id, parent)
	VALUES (%s, %s, %s, %s, %s)
'''

th_insert_query = f'''
	INSERT INTO raw_data.tteok_ham (tteok_ham_No, item_type, item_detailed_type, tteok_ham_type, group_No, shelf_life, months, tteok_ham_title, tteok_ham_subtitle1, tteok_ham_subtitle2, tteok_ham_subtitle3, subjects, isshow, tteok_ham_price)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

student_insert_query = f'''
	INSERT INTO raw_data.student (user_No, term_grade_id, parent_phone_number, parent_name, year)
	VALUES (%s, %s, %s, %s, %s)
'''

scs_insert_query = f'''
	INSERT INTO raw_data.student_change_subject_v2 (change_subject_No, lecture_change_form_No, payed_check_list, is_same_teacher, application_datetime, remark_No, remark, selected_subject, change_option_value, month_option, create_datetime, update_datetime)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

lcf_insert_query = f'''
	INSERT INTO raw_data.lecture_change_form (lecture_change_form_No, lecture_vt_No, integration_No, form_type, process_status, process_failed_reason, create_datetime, update_datetime)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

lvts_insert_query = f'''
	INSERT INTO raw_data.lecture_vt_schedules (schedule_No, follow_No, lecture_vt_No, lecture_cycle_No, stage_count, cycle_count, is_free, offer_type, schedule_state, tutoring_datetime, last_tutoring_datetime, create_datetime, update_datetime, cycle_payment_item, per_done_month)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

ltvt_insert_query = f'''
	INSERT INTO raw_data.lecture_teacher_vt (lecture_teacher_vt_No, teacher_user_No, lecture_vt_No, last_schedule_No, teacher_vt_status, academic_departments, teacher_academic_major, division_of_matching_standard, active_done_month, total_done_month, reactive_at, create_at, update_at, lecture_subject_id)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

payment_insert_query = f'''
	INSERT INTO raw_data.payment (payment_No, user_No, LGD_AMOUNT, LGD_BUYER, order_id, LGD_TID, LGD_PAYTYPE, TTEOK_HAM_No, LGD_PRODUCTINFO, payment_regdate, memo, lecture_vt_No, supply_value, additional_tax, cancelled_supply_value, cancelled_additional_tax, completed_at, discounted_value, cancelled_pg_tid, cancelled_amount, cancelled_pg_fee, state, original_payment_id, payment_method)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

scph_insert_query = f'''
	INSERT INTO raw_data.student_change_pause_history (change_pause_history_No,lecture_change_form_No,lecture_vt_No
	,active_count,total_done_month,rent_total_done_month,own_total_done_month
    ,device_total_done_month,rent_type,option1,used_type
    ,process_status,create_datetime,student_type,tutoring_state
    ,name,user_No,subject,teacher_name,teacher_user_No,item_type,TTEOK_HAM_TYPE
    ,reason,reason_detail,teacher_review_content,update_datetime
    ,phone_number,parent_phone_number,teacher_phone_number,form_count,stage_offer_cycle_count,stage_pre_offer_cycle_count)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

school_insert_query = f'''
	INSERT INTO raw_data.school (seq, schoolName, schoolGubun, schoolType, estType, region, link, total_count)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

las_insert_query = f'''
	INSERT INTO raw_data.lecture_application_students (lecture_vt_no, "학생번호", "학교명", "희망전공", "학년", "주소", "지역", "희망성별", "신청_동기", "다른학교튜터가능", "신청 ipad 색상")
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

teacher_insert_query = f'''
	INSERT INTO raw_data.teacher (user_No, teacher_school_subject, hakbun, univ_graduate_type, graduate_highschool_seq, seoltab_tutoring_ON_OFF, selected_subjects, seoltab_state, seoltab_state_updateAT)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
'''


school_university_insert_query = f'''
	INSERT INTO raw_data.school_university (seq, schoolName, campusName, schoolGubun, schoolType, schoolEmail, estType, region, link, total_count, is_show)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

stc_insert_query = f'''
	INSERT INTO raw_data.seoltab_teacher_config (user_No, TUTORING_PR)
	VALUES (%s, %s)
'''

lt_insert_query = f'''
	INSERT INTO raw_data.lecture_tutor (user_No, rental_fee_type, create_datetime, active_count)
	VALUES (%s, %s, %s, %s)
'''

account_insert_query = f'''
	INSERT INTO raw_data.account ( user_No, cash, point)
	VALUES (%s, %s, %s)
'''

lvt_delete_query = '''
delete from raw_data.lecture_video_tutoring;

commit;
'''

user_delete_query = '''
delete from raw_data."user";

commit;
'''

ttn_delete_query = '''
delete from raw_data.term_taxonomy_name;

commit;
'''

lvts_delete_query = '''
delete from raw_data.lecture_vt_schedules;

commit;
'''

th_delete_query = '''
delete from raw_data.tteok_ham;

commit;
'''

student_delete_query = '''
delete from raw_data.student;

commit;
'''

ltvt_delete_query = '''
delete from raw_data.lecture_teacher_vt;

commit;
'''

scs_delete_query = '''
delete from raw_data.student_change_subject_v2;

commit;
'''

lcf_delete_query = '''
delete from raw_data.lecture_change_form;

commit;
'''

payment_delete_query = '''
delete from raw_data.payment;

commit;
'''

scph_delete_query = '''
delete from raw_data.student_change_pause_history;

commit;
'''

school_university_delete_query = '''
delete from raw_data."school_university";

commit;
'''

las_delete_query = '''
delete from raw_data."lecture_application_students";

commit;
'''

teacher_delete_query = '''
delete from raw_data."teacher";

commit;
'''

school_delete_query = '''
delete from raw_data."school";

commit;
'''

stc_delete_query = '''
delete from raw_data."seoltab_teacher_config";

commit;
'''

lt_delete_query = '''
delete from raw_data."lecture_tutor";

commit;
'''

account_delete_query = '''
delete from raw_data."account";

commit;
'''
