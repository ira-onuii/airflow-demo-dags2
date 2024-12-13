from datetime import datetime, timedelta

date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

table_name = f'"{date}"'



aub_select_query = '''
select id
	,createdat
	,updatedat
	,deletedat
	,userid
	,benefitid
from active_user_benefit
'''

address_select_query = '''
select 
	id
	,createdat
	,updatedat
	,deletedat
	,name
	,orderername
	,phonenumber
	,postcode
	,address
	,detailedaddress
	,userid
	,isdefault
	,isrecentlyused
from address
'''

user_select_query = '''
select 
from 
'''

benefit_select_query = '''
select 
id
,createdat
,updatedat
,deletedat
,name
,type
,isavailable
,version
,detailedtype
from benefit
'''

billing_card_select_query = '''
select 
id
,createdat
,updatedat
,deletedat
,userid
,name
,number
,companyname
,companycode
,isrecentlyused
,ismain
,type
,islegacy
from billing_card
'''

billing_card_key_select_query = '''
select 
,id
,uid
,pg
,billingcardid
from billing_card_key
'''

block_select_query = '''
select 
id
,uid
,type
,name
,istaxed
,price
,defaultprice
,paymentitemid
from block
'''

block_refund_select_query = '''
select 
id
,refundedat
,requestedat
,state
,memo
,amount
,paymentitemid
,blockid
,detailid
,refundbankid
,refundmethod
from block_refund
'''



block_refund_detail_select_query = '''
select 
id
,receipturl
,pgtid
,supplyvalue
,amount
,additionaltax
,paymentid
from block_refund_detail
'''



block_refund_reason_select_query = '''
select 
blockrefundid
, refundreasonid
from block_refund_reason
'''

book_select_query = '''
select 
id
,publisher
,blockid
,isbn10
,isbn13
from book
'''

career_select_query = '''
select 
id
,teacher_id
,major_id
,subject_id
,target
,month
,created_at
,updated_at
from career
'''

cashout_history_select_query = '''
select 
id
,user_id
,transaction_id
,withdraw_point
,tax_applied_cash
,tax_applied_withdraw_cash
,created_at
,updated_at
,phone
,name
,local_income_tax_fee
,income_tax_fee
,teacher_id
from cashout_history
'''

change_lecture_history_select_query = '''
select 
changed_lecture_id
,created_at
,created_by
,id
,prev_lecture_id
,updated_at
from change_lecture_history
'''

contract_select_query = '''
select 
id
,createdat
,updatedat
,deletedat
,name
,month
,ownershiptype
,isused
,memo
,devicetype
,devicegeneration
,memo2
from contract
'''

course_select_query = '''
select 
id
,teacher_id
,subject_id
,is_deleted
,created_at
,updated_at
,subject_detail_id
from course
'''

discount_select_query = '''
select 
id
,type
,value
,method
,benefitid
from discount
'''

division_select_query = '''
select 
id
,tid
,division
,seoltab_division
,type
,updated_at
,created_at
from division
'''

extended_lecture_payment_option_select_query = '''
select 
id
,createdat
,updatedat
,deletedat
,lecturecontextid
,userid
,installmentperiod
from
'''

grade_select_query = '''
select 
id
,name
from grade
'''

interest_free_installment_select_query = '''
select 
id
,createdat
,updatedat
,deletedat
,name
,freemonth
,pg
from interest_free_installment
'''


lecture_select_query = '''
select 
is_single
,created_at
,end_date_time
,id
,latest_lecture_cycle_id
,latest_round_id
,manager_id
,start_date_time
,updated_at
,latest_fixed_package_id
,status
,subject_codes
from lecture
'''

lecture_cycle_select_query = '''
select 
idx
,minutes_per_round
,next_total_month
,total_rounds_of_free
,total_rounds_of_pay
,used_rounds_of_free
,used_rounds_of_pay
,created_at
,id
,latest_round_id
,lecture_id
,updated_at
,fixed_package_id
from lecture_cycle
'''

lecture_cycle_schedule_select_query = '''
select 
start_time_id
,created_at
,id
,lecture_cycle_id
,updated_at
from lecture_cycle_schedule
'''

lecture_cycle_user_select_query = '''
select 
installment_period_of_next_payment
,is_student
,created_at
,exit_date_time
,id
,join_date_time
,latest_ticket_id
,latest_ticket_wallet_id
,latest_ticket_wallet_payment_id
,lecture_id
,updated_at
,user_id
from lecture_cycle_user
'''

lecture_payment_option_select_query = '''
select 
id
,createdat
,updatedat
,deletedat
,lecturecontextid
,installmentperiod
from lecture_payment_option
'''

matching_condition_category_select_query = '''
select 
created_at
,id
,updated_at
,name
,value
from matching_condition_category
'''

matching_request_select_query = '''
select 
version
,accepted_suggestion_id
,created_at
,created_by
,desired_start_date_time
,id
,latest_lecture_id
,student_id
,updated_at
,learning_concern
,memo_for_admin
,memo_for_reference
,memo_to_show_teacher
,desired_time_ids
,status
,subject_codes
from matching_request
'''

matching_request_condition_select_query = '''
select 
id
,created_at
,updated_at
,value
,matching_condition_category_id
,matching_request_id
from matching_request_condition
''' 

matching_request_lecture_select_query = '''
select 
created_at
,id
,lecture_id
,matching_request_id
,updated_at
from matching_request_lecture
'''

matching_suggestion_select_query = '''
select 
version
,accepted_at
,canceled_at
,created_at
,created_by
,expired_at
,id
,matching_request_id
,refused_at
,teacher_id
,updated_at
,viewed_at
,memo_for_teacher
,status
,time_ids
from matching_suggestion
'''

parent_child_select_query = '''
select 
id
,parent_user_id
,student_user_id
,status
from parent_child
'''

payment_select_query = '''
select 
id
,createdat
,updatedat
,paidat
,refundedat
,failedat
,impuid
,state
,paymentmethod
,paymentroute
,price
,deliveryfee
,refundedamount
,merchantuid
,installmentperiod
,pg
,pgtid
,applynumber
,receipturl
,memo
,userid
,discountedvalue
,addressid
,failreason
,version
from payment
'''

payment_item_select_query = '''
select 
id
,createdat
,updatedat
,deletedat
,type
,name
,price
,taxfreeamount
,discountedvalue
,supplyvalue
,additionaltax
,refundedamount
,memo
,uid
,userid
,count
,isaddedtocart
,paymentid
from payment_item
'''

payment_item_benefit_select_query = '''
select 
paymentitemid
,benefitid
,value
from payment_item_benefit
'''

payment_item_discount_select_query = '''
select 
id
,createdat
,name
,discountedvalue
,benefitid
,paymentitemid
from payment_item_discount
'''

payment_item_extra_info_select_query = '''
select 
id
,tteokhamid
,paymentitemid
,subjectid
,lvtid
,legacyitemid
from payment_item_extra_info
'''

payment_settlement_select_query = '''
select 
id
,settlementday
,payoutamount
,pgfee
,type
,paymentid
,refunddetailid
from payment_settlement
'''

payment_used_billing_card_select_query = '''
select 
billingcardid
,paymentid
from payment_used_billing_card
'''

refund_bank_select_query = '''
select 
id
,createdat
,updatedat
,deletedat
,name
,holder
,code
,accountnumber
,isavailable
,userid
from refund_bank
'''

refund_reason_select_query = '''
select 
id
,createdat
,updatedat
,deletedat
,target
,description
,isdisplayed
,isfreedeliveryfee
from refund_reason
'''

register_path_select_query = '''
select 
id
,user_id
,path
,device
,created_at
from register_path
'''

room_select_query = '''
select 
created_at
,deleted_at
,id
,round_id
,updated_at
,streaming_channel_id
,type
from room
'''
room_error_report_select_query = '''
select 
created_at
,created_by
,id
,room_id
,updated_at
,reason
,type
from room_error_report
'''

room_participant_select_query = '''
select 
created_at
,exited
,id
,room_id
,updated_at
,user_id
,member_id
,role
from room_participant
'''

round_select_query = '''
select
idx
,created_at
,created_by
,id
,lecture_cycle_id
,reserved_end_date_time
,reserved_start_date_time
,teacher_id
,updated_at
,flow
,provider
,status 
from round
'''

round_study_time_select_query = '''
select 
id
,created_at
,end_end_time
,round_id
,start_date_time
,updated_at
from round_study_time
'''

school_select_query = '''
select 
id
seq
name
category
type
establish
region
address
link
from school
'''

seoltab_content_select_query = '''
select  
id
,created_at
,seoltab_content_group_id
,updated_at
,content_id
,content_link
,handwriting_data_id
,content_type
,handwriting_data_type
from seoltab_content
'''

settlement_config_select_query = '''
select 
id
,teacher_id
,created_at
,updated_at
,is_lock
,bank_code
,bank_name
,account_number
,owner
from settlement_config
'''

single_lecture_line_select_query = '''
select 
id
,done_month
,created_at
,latest_lecture_id
,student_id
,teacher_id
,updated_at
,code
from single_lecture_line
'''

single_lecture_line_lecture_select_query = '''
select
id
,created_at
,lecture_id
,single_lecture_line_id
,updated_at
from single_lecture_line_lecture
'''

student_select_query = '''
select 
id
,user_id
,year
,education_stage
,mbti
,school_id
,hope_major_id
,created_at
,updated_at
from student
'''

student_content_select_query = '''
select 
id
,idx
,bookmarked_at
,created_at
,ticket_id
,updated_at
,content_id
,content_link
,handwriting_data_id
,content_type
,handwriting_data_type
from student_content
'''

student_grade_select_query = '''
select 
id
,student_id
,grade_id
,subject_id
,subject_name
,created_at
,updated_at
from student_grade
'''

student_homework_content_select_query = '''
select 
id
,is_done
,student_question_content_id
from student_homework_content
'''

student_hope_major_select_query = '''
select 
id
,student_id
,university_major_id
from student_hope_major
'''

student_review_select_query = '''
select 
id
,is_change_teacher
,net_promoter_score
,created_at
,ticket_id
,updated_at
,checks
,negative_tags
from student_review
'''

subject_select_query = '''
select 
id
,tid
,name
,is_deleted
,created_at
,updated_at
from subject
'''

subject_detail_select_query = '''
select 
id
,subject_id
,code
,tid
,is_used_teacher
,created_at
,updated_at
,name
from subject_detail
'''

teacher_select_query = '''
select 
id
,user_id
,university_information_id
,university_email
,seoltab_state
,grade
,introduction
,penalty
,hakbun
,university_state
,active_at
,application_passed_at
,created_at
,updated_at
,graduate_highschool_type
,enrollment_type
,matching_available
,encrypt_jumin
,is_deleted
,highschool_id
from teacher
'''

teacher_progress_content_select_query = '''
select 
id
,idx
,created_at
,round_id
,ticket_id
,updated_at
,content_id
,content_link
,handwriting_data_id
,content_type
,handwriting_data_type
from teacher_progress_content
'''

teacher_review_select_query = '''
select 
id
,created_at
,ticket_id
,updated_at
,summary_note
,negative_tags
,positive_tags
from teacher_review
'''

temp_matching_request_select_query = '''
select 
id
,created_at
,lecture_id
,matching_request_id
,student_id
,submitted_at
,updated_at
,uuid
,data
from temp_matching_request
'''

template_content_select_query = '''
select 
id
,is_sync
,created_at
,template_content_group_id
,updated_at
,content_id
,content_link
,content_type
from template_content
'''

term_select_query = '''
select 
id
,title
,content
,version
,effective_date
,created_at
,updated_at
from term
'''

ticket_select_query = '''
select 
id
,idx
,is_free
,created_at
,round_id
,ticket_wallet_id
,updated_at
,user_id
,status
from ticket
'''

ticket_for_student_select_query = '''
select  
id
,homework_is_submitted
,created_at
,homework_is_checked_by
,updated_at
from ticket_for_student
'''

ticket_for_teacher_select_query = '''
select 
id
,progress_is_submitted
,created_at
,updated_at
from ticket_for_teacher
'''

ticket_study_time_select_query = '''
select 
id
,created_at
,in_date_time
,out_date_time
,ticket_id
,updated_at
from ticket_study_time
'''

ticket_wallet_select_query = '''
select 
id
,is_active
,total_tickets_of_free
,total_tickets_of_pay
,used_tickets_of_free
,used_tickets_of_pay
,created_at
,latest_ticket_id
,latest_ticket_wallet_payment_id
,lecture_cycle_id
,lecture_user_id
,payed_at
,updated_at
,user_id
from ticket_wallet
'''

ticket_wallet_payment_select_query = '''
select 
id
,is_success
,created_at
,payment_block_id
,ticket_wallet_id
,updated_at
from ticket_wallet_payment
'''

time_select_query = '''
select 
id
,month
,week
,minute
,paymentitemid
from time
'''

university_select_query = '''
select 
id
,name
,email_suffix
,region
,type
,created_at
,updated_at
,old_id
from university
'''

university_information_select_query = '''
select 
id
,department
,major
,division_id
,university_id
,seoltab_pass
,created_at
,updated_at
from university_information
'''

user_select_query = '''
select 
id
,name
,code
,email
,password
,phone_number
,gender
,status
,actor
,legacy_user_id
,birth_date
,ci
,created_at
,updated_at
,delete_at
,latest_login_at
from user
''' 

user_address_select_query = '''
select 
id
,user_id
,post_code
,address1
,address2
,created_at
,updated_at
from user_address
'''

user_contract_select_query = '''
select 
id
,createdat
,updatedat
,deletedat
,userid
,paidat
,isconfirmed
,paymentid
,deviceid
,contractid
from user_contract
'''

user_device_select_query = '''
select 
id
,user_id
,device
,token
,created_at
,updated_at
from user_device
'''

user_protector_select_query = '''
select 
id
,user_id
,name
,phone_number
from user_protector
'''

user_push_select_query = '''
select 
id
,user_id
,type
,is_opt_in
,opt_in_date
from user_push
'''

user_term_agreement_select_query = '''
select 
id
,user_id
,term_id
,agreement_date
from user_term_agreement
'''

user_time_line_select_query = '''
select 
id
,is_teacher
,version
,created_at
,updated_at
,user_id
,available_time_ids
,desired_time_ids
from user_time_line
'''

user_using_time_line_select_query = '''
select 
id
,created_at
,lecture_id
,updated_at
,user_id
,user_time_line_id
,time_ids
from user_using_time_line
'''

vbank_select_query = '''
select 
id
,createdat
,name
,code
,accountnumber
,duedate
,iscancelled
,paymentid
from vbank
'''



lvt_insert_query = f'''
	INSERT INTO raw_data.lecture_video_tutoring (lecture_vt_no,student_user_no,lecture_subject_id,student_type,tutoring_state,payment_item,next_payment_item,current_schedule_no,stage_max_cycle_count,stage_free_cycle_count,stage_pre_offer_cycle_count,stage_offer_cycle_count,create_datetime,update_datetime,last_done_datetime,application_datetime,memo,total_subject_done_month,reactive_datetime)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''


user_insert_query = f'''
	INSERT INTO raw_data."user" (user_No, term_user_type, user_status, email_id, nickname, name, phone_number, device, school_seq, sex, birth_year, recent_login_time, join_datetime, login_version, login_device, update_datetime)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
	INSERT INTO raw_data.lecture_application_students (submitted_at, lecture_vt_no, "학생번호", "학교명", "희망전공", "학년", "주소", "지역", "희망성별", "신청_동기", "다른학교튜터가능", "신청 ipad 색상")
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

matching_insert_query = f'''
	INSERT INTO raw_data.matching (matching_id
	, "type"
	, mlvt_status 
	, teachersuggestionstatus 
	, lecture_vt_No 
	, student_id 
	, student_name 
	, student_year 
	, student_grade 
	, student_gender 
	, subject_id 
	, subject 
	, "option" 
	, itemmonth 
	, manager_No 
	, manager_name 
	, application 
	, mlvt_created_at 
	, mlvt_updated_at 
	, memo 
	, note 
	, ms_status 
	, suggestion_teacher_id 
	, suggestion_teacher_name 
	, ms_created_at 
	, ms_updated_at 
	, suggestedat 
	, refusedreason)
	VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s)
'''

contract_teacher_insert_query = f'''
	INSERT INTO raw_data.contract_teacher ("no","user_No","contract_No",contract_name,contract_status,contract_send_status,update_datetime,create_datetime,sign_datetime,cancel_reason,send_datetime)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

lvtsh_insert_query = f'''
	INSERT INTO raw_data.lecture_vt_schedules_history ("history_No","schedule_No","change_type","time_type","tutoring_datetime","reason","alert_count","create_datetime","last_tutoring_datetime")
	VALUES (%s, %s,%s, %s,%s, %s,%s, %s, %s)
'''

sf_insert_query = f'''
	INSERT INTO raw_data.student_follow (follow_No, student_user_No, teacher_user_No)
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

matching_delete_query = '''
delete from raw_data.matching;

commit;
'''

contract_teacher_delete_query = '''
delete from raw_data.contract_teacher;

commit;
'''

lvtsh_delete_query = '''
delete from raw_data.lecture_vt_schedules_history;

commit;
'''

sf_delete_query = '''
delete from raw_data.student_follow;

commit;
'''