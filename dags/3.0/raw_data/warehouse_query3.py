from datetime import datetime, timedelta


date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

table_name = f'"{date}"'



active_user_benefit_select_query = f'''
select id
	,createdat
	,updatedat
	,deletedat
	,userid
	,benefitid
from payment_live_mysql.payment.active_user_benefit
-- where updatedat > cast(date(now()) - interval '1' day as timestamp)
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
from payment_live_mysql.payment.address
-- where updatedat > cast(date(now()) - interval '1' day as timestamp)
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
from payment_live_mysql.payment.benefit
-- where updatedat > cast(date(now()) - interval '1' day as timestamp)
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
from payment_live_mysql.payment.billing_card
-- where updatedat > cast(date(now()) - interval '1' day as timestamp)
'''

billing_card_key_select_query = '''
select 
id
,uid
,pg
,billingcardid
from payment_live_mysql.payment.billing_card_key
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
from payment_live_mysql.payment.block
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
from payment_live_mysql.payment.block_refund
-- where refundedat > cast(date(now()) - interval '1' day as timestamp)
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
from payment_live_mysql.payment.block_refund_detail
'''



block_refund_reason_select_query = '''
select 
blockrefundid
, refundreasonid
from payment_live_mysql.payment.block_refund_reason
'''

book_select_query = '''
select 
id
,publisher
,blockid
,isbn10
,isbn13
from payment_live_mysql.payment.book
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
from payment_live_mysql.payment.contract
-- where updatedat > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
'''

discount_select_query = '''
select 
id
,type
,value
,method
,benefitid
from payment_live_mysql.payment.discount
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
from payment_live_mysql.payment.extended_lecture_payment_option
-- where updatedat > cast(date(now()) - interval '1' day as timestamp)
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
from payment_live_mysql.payment.interest_free_installment
-- where updatedat > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
'''

lecture_cycle_schedule_select_query = '''
select 
start_time_id
,created_at
,id
,lecture_cycle_id
,updated_at
from lecture_cycle_schedule
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
'''

lecture_payment_option_select_query = '''
select 
id
,createdat
,updatedat
,deletedat
,lecturecontextid
,installmentperiod
from payment_live_mysql.payment.lecture_payment_option
-- where updatedat > cast(date(now()) - interval '1' day as timestamp)
'''

matching_condition_category_select_query = '''
select 
created_at
,id
,updated_at
,name
,value
from matching_condition_category
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
''' 

matching_request_lecture_select_query = '''
select 
created_at
,id
,lecture_id
,matching_request_id
,updated_at
from matching_request_lecture
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
from payment_live_mysql.payment.payment
-- where updatedat > cast(date(now()) - interval '1' day as timestamp)
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
from payment_live_mysql.payment.payment_item
-- where updatedat > cast(date(now()) - interval '1' day as timestamp)
'''

payment_item_benefit_select_query = '''
select 
paymentitemid
,benefitid
,value
from payment_live_mysql.payment.payment_item_benefit
'''

payment_item_discount_select_query = '''
select 
id
,createdat
,name
,discountedvalue
,benefitid
,paymentitemid
from payment_live_mysql.payment.payment_item_discount
'''

payment_item_extra_info_select_query = '''
select 
id
,tteokhamid
,paymentitemid
,subjectid
,lvtid
,legacyitemid
from payment_live_mysql.payment.payment_item_extra_info
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
from payment_live_mysql.payment.payment_settlement
'''

payment_used_billing_card_select_query = '''
select 
billingcardid
,paymentid
from payment_live_mysql.payment.payment_used_billing_card
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
from payment_live_mysql.payment.refund_bank
-- where updatedat > cast(date(now()) - interval '1' day as timestamp)
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
from payment_live_mysql.payment.refund_reason
-- where updatedat > cast(date(now()) - interval '1' day as timestamp)
'''

register_path_select_query = '''
select 
id
,user_id
,path
,device
,created_at
from register_path
-- where created_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
'''

single_lecture_line_lecture_select_query = '''
select
id
,created_at
,lecture_id
,single_lecture_line_id
,updated_at
from single_lecture_line_lecture
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
'''

ticket_for_student_select_query = '''
select  
id
,homework_is_submitted
,created_at
,homework_is_checked_by
,updated_at
from ticket_for_student
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
'''

ticket_for_teacher_select_query = '''
select 
id
,progress_is_submitted
,created_at
,updated_at
from ticket_for_teacher
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
'''

time_select_query = '''
select 
id
,month
,week
,minute
,paymentitemid
from payment_live_mysql.payment.time
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
,deleted_at
-- ,latest_login_at
from "3.0_user_mysql"."user"."user"
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
from payment_live_mysql.payment.user_contract
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
-- where updated_at > cast(date(now()) - interval '1' day as timestamp)
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
from payment_live_mysql.payment.vbank
-- where createdat > cast(date(now()) - interval '1' day as timestamp)
'''



active_user_benefit_insert_query = f'''
	INSERT INTO raw_data.active_user (id
	,createdat
	,updatedat
	,deletedat
	,userid
	,benefitid)
	VALUES (%s, %s, %s, %s, %s, %s)
'''

address_insert_query = f'''
	INSERT INTO raw_data.address (id
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
	,isrecentlyused)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

benefit_insert_query = f'''
	INSERT INTO raw_data.benefit (
id
,createdat
,updatedat
,deletedat
,name
,type
,isavailable
,version
,detailedtype)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

billing_card_insert_query = f'''
	INSERT INTO raw_data.billing_card (id
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
,islegacy)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

billing_card_key_insert_query = f'''
	INSERT INTO raw_data.billing_card_key (
,id
,uid
,pg
,billingcardid)
	VALUES (%s, %s, %s, %s)
'''

block_insert_query = f'''
	INSERT INTO raw_data.block (id
,uid
,type
,name
,istaxed
,price
,defaultprice
,paymentitemid)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

block_refund_insert_query = f'''
	INSERT INTO raw_data.block_refund (id
,refundedat
,requestedat
,state
,memo
,amount
,paymentitemid
,blockid
,detailid
,refundbankid
,refundmethod)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

block_refund_detail_insert_query = f'''
	INSERT INTO raw_data.block_refund_detail (id
,receipturl
,pgtid
,supplyvalue
,amount
,additionaltax
,paymentid)
	VALUES (, %s, %s, %s, %s, %s, %s, %s)
'''

block_refund_reason_insert_query = f'''
	INSERT INTO raw_data.block_refund_reason (blockrefundid
, refundreasonid)
	VALUES (%s, %s)
'''

book_insert_query = f'''
	INSERT INTO raw_data.book (id
,publisher
,blockid
,isbn10
,isbn13)
	VALUES (%s, %s, %s, %s, %s)
'''

career_insert_query = f'''
	INSERT INTO raw_data.career (id
,teacher_id
,major_id
,subject_id
,target
,month
,created_at
,updated_at)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

cashout_history_insert_query = f'''
	INSERT INTO raw_data.cashout_history (id
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
,teacher_id)
	VALUES (, %s)
'''

change_lecture_history_insert_query = f'''
	INSERT INTO raw_data.change_lecture_history (changed_lecture_id
,created_at
,created_by
,id
,prev_lecture_id
,updated_at)
	VALUES (%s, %s, %s, %s, %s, %s)
'''

contract_insert_query = f'''
	INSERT INTO raw_data.contract (id
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
,memo2)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

course_insert_query = f'''
	INSERT INTO raw_data.course (id
,teacher_id
,subject_id
,is_deleted
,created_at
,updated_at
,subject_detail_id)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

discount_insert_query = f'''
	INSERT INTO raw_data.discount (id
,type
,value
,method
,benefitid)
	VALUES (%s, %s, %s, %s, %s)
'''

division_insert_query = f'''
	INSERT INTO raw_data.division (id
,tid
,division
,seoltab_division
,type
,updated_at
,created_at)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

extended_lecture_payment_option_insert_query = f'''
	INSERT INTO raw_data.extended_lecture_payment_option (id
,createdat
,updatedat
,deletedat
,lecturecontextid
,userid
,installmentperiod)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

grade_insert_query = f'''
	INSERT INTO raw_data.grade (id
,name)
	VALUES (%s, %s)
'''

interest_free_installment_insert_query = f'''
	INSERT INTO raw_data.interest_free_installment (id
,createdat
,updatedat
,deletedat
,name
,freemonth
,pg)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

lecture_insert_query = f'''
	INSERT INTO raw_data.lecture (is_single
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
,subject_codes)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

lecture_cycle_insert_query = f'''
	INSERT INTO raw_data.lecture_cycle (idx
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
,fixed_package_id)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

lecture_cycle_schedule_insert_query = f'''
	INSERT INTO raw_data.lecture_cycle_schedule (start_time_id
,created_at
,id
,lecture_cycle_id
,updated_at)
	VALUES (%s, %s, %s, %s, %s)
'''

lecture_cycle_user_insert_query = f'''
	INSERT INTO raw_data.lecture_cycle_user (installment_period_of_next_payment
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
,user_id)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

lecture_payment_option_insert_query = f'''
	INSERT INTO raw_data.lecture_payment_option (id
,createdat
,updatedat
,deletedat
,lecturecontextid
,installmentperiod)
	VALUES (%s, %s, %s, %s, %s, %s)
'''

matching_condition_category_insert_query = f'''
	INSERT INTO raw_data. (created_at
,id
,updated_at
,name
,value)
	VALUES (%s, %s, %s, %s, %s)
'''

matching_request_insert_query = f'''
	INSERT INTO raw_data.matching_request (version
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
,subject_codes)
	VALUES (, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

matching_request_condition_insert_query = f'''
	INSERT INTO raw_data.matching_request_condition (id
,created_at
,updated_at
,value
,matching_condition_category_id
,matching_request_id)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

matching_request_lecture_insert_query = f'''
	INSERT INTO raw_data.matching_request_lecture (created_at
,id
,lecture_id
,matching_request_id
,updated_at)
	VALUES (, %s, %s, %s, %s, %s)
'''

matching_suggestion_insert_query = f'''
	INSERT INTO raw_data.matching_suggestion (version
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
,time_ids)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

parent_child_insert_query = f'''
	INSERT INTO raw_data.parent_child (id
,parent_user_id
,student_user_id
,status)
	VALUES (%s, %s, %s, %s)
'''

payment_insert_query = f'''
	INSERT INTO raw_data.payment (id
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
,version)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

payment_item_insert_query = f'''
	INSERT INTO raw_data.payment_item (id
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
,paymentid)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

payment_item_benefit_insert_query = f'''
	INSERT INTO raw_data.payment_item_benefit (paymentitemid
,benefitid
,value)
	VALUES (%s, %s, %s)
'''

payment_item_discount_insert_query = f'''
	INSERT INTO raw_data.payment_item_discount (id
,createdat
,name
,discountedvalue
,benefitid
,paymentitemid)
	VALUES (%s, %s, %s, %s, %s, %s)
'''

payment_item_extra_info_insert_query = f'''
	INSERT INTO raw_data.payment_item_extra_info (id
,tteokhamid
,paymentitemid
,subjectid
,lvtid
,legacyitemid)
	VALUES (%s, %s, %s, %s, %s, %s)
'''

payment_settlement_insert_query = f'''
	INSERT INTO raw_data.payment_settlement (id
,settlementday
,payoutamount
,pgfee
,type
,paymentid
,refunddetailid)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

payment_used_billing_card_insert_query = f'''
	INSERT INTO raw_data.payment_used_billing_card (billingcardid
,paymentid)
	VALUES (%s, %s)
'''

refund_bank_insert_query = f'''
	INSERT INTO raw_data.refund_bank (id
,createdat
,updatedat
,deletedat
,name
,holder
,code
,accountnumber
,isavailable
,userid)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

refund_reason_insert_query = f'''
	INSERT INTO raw_data.refund_reason (id
,createdat
,updatedat
,deletedat
,target
,description
,isdisplayed
,isfreedeliveryfee)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

register_path_insert_query = f'''
	INSERT INTO raw_data.register_path (id
,user_id
,path
,device
,created_at)
	VALUES (%s, %s, %s, %s, %s)
'''

room_insert_query = f'''
	INSERT INTO raw_data.room (created_at
,deleted_at
,id
,round_id
,updated_at
,streaming_channel_id
,type)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

room_error_report_insert_query = f'''
	INSERT INTO raw_data.room_error_report (created_at
,created_by
,id
,room_id
,updated_at
,reason
,type)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

room_participant_insert_query = f'''
	INSERT INTO raw_data.room_participant (created_at
,exited
,id
,room_id
,updated_at
,user_id
,member_id
,role)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

round_insert_query = f'''
	INSERT INTO raw_data.round (idx
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
,status)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

round_study_time_insert_query = f'''
	INSERT INTO raw_data.round_study_time (id
,created_at
,end_end_time
,round_id
,start_date_time
,updated_at)
	VALUES (%s, %s, %s, %s, %s, %s)
'''

school_insert_query = f'''
	INSERT INTO raw_data.school (id
seq
name
category
type
establish
region
address
link)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

seoltab_content_insert_query = f'''
	INSERT INTO raw_data.seoltab_content (id
,created_at
,seoltab_content_group_id
,updated_at
,content_id
,content_link
,handwriting_data_id
,content_type
,handwriting_data_type)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

settlement_config_insert_query = f'''
	INSERT INTO raw_data.settlement_config (id
,teacher_id
,created_at
,updated_at
,is_lock
,bank_code
,bank_name
,account_number
,owner)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

single_lecture_line_insert_query = f'''
	INSERT INTO raw_data.single_lecture_line (id
,done_month
,created_at
,latest_lecture_id
,student_id
,teacher_id
,updated_at
,code)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

single_lecture_line_lecture_insert_query = f'''
	INSERT INTO raw_data.single_lecture_line_lecture (id
,created_at
,lecture_id
,single_lecture_line_id
,updated_at)
	VALUES (%s, %s, %s, %s, %s)
'''

student_insert_query = f'''
	INSERT INTO raw_data.student (id
,user_id
,year
,education_stage
,mbti
,school_id
,hope_major_id
,created_at
,updated_at)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

student_content_insert_query = f'''
	INSERT INTO raw_data.student_content (id
,idx
,bookmarked_at
,created_at
,ticket_id
,updated_at
,content_id
,content_link
,handwriting_data_id
,content_type
,handwriting_data_type)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

student_grade_insert_query = f'''
	INSERT INTO raw_data.student_grade (id
,student_id
,grade_id
,subject_id
,subject_name
,created_at
,updated_at)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
'''
student_homework_content_insert_query = f'''
	INSERT INTO raw_data.student_homework_content (id
,is_done
,student_question_content_id)
	VALUES (%s, %s, %s)
'''

student_hope_major_insert_query = f'''
	INSERT INTO raw_data.student_hope_major (id
,student_id
,university_major_id)
	VALUES (%s, %s, %s)
'''

student_review_insert_query = f'''
	INSERT INTO raw_data.student_review (id
,is_change_teacher
,net_promoter_score
,created_at
,ticket_id
,updated_at
,checks
,negative_tags)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

subject_insert_query = f'''
	INSERT INTO raw_data.subject (id
,tid
,name
,is_deleted
,created_at
,updated_at)
	VALUES (%s, %s, %s, %s, %s, %s)
'''

subject_detail_insert_query = f'''
	INSERT INTO raw_data.subject_detail (id
,subject_id
,code
,tid
,is_used_teacher
,created_at
,updated_at
,name)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

teacher_insert_query = f'''
	INSERT INTO raw_data.teacher (id
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
,highschool_id)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

teacher_progress_content_insert_query = f'''
	INSERT INTO raw_data.teacher_progress_content (id
,idx
,created_at
,round_id
,ticket_id
,updated_at
,content_id
,content_link
,handwriting_data_id
,content_type
,handwriting_data_type)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

teacher_review_insert_query = f'''
	INSERT INTO raw_data.teacher_review (id
,created_at
,ticket_id
,updated_at
,summary_note
,negative_tags
,positive_tags)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

temp_matching_request_insert_query = f'''
	INSERT INTO raw_data.temp_matching_request (id
,created_at
,lecture_id
,matching_request_id
,student_id
,submitted_at
,updated_at
,uuid
,data)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

template_content_insert_query = f'''
	INSERT INTO raw_data.template_content (id
,is_sync
,created_at
,template_content_group_id
,updated_at
,content_id
,content_link
,content_type)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''


term_insert_query = f'''
	INSERT INTO raw_data.term (id
,title
,content
,version
,effective_date
,created_at
,updated_at)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

ticket_insert_query = f'''
	INSERT INTO raw_data.ticket (id
,idx
,is_free
,created_at
,round_id
,ticket_wallet_id
,updated_at
,user_id
,status)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

ticket_for_student_insert_query = f'''
	INSERT INTO raw_data.ticket_for_student (id
,homework_is_submitted
,created_at
,homework_is_checked_by
,updated_at)
	VALUES (%s, %s, %s, %s, %s)
'''


ticket_for_teacher_insert_query = f'''
	INSERT INTO raw_data.ticket_for_teacher (id
,progress_is_submitted
,created_at
,updated_at)
	VALUES (%s, %s, %s, %s)
'''

ticket_study_time_insert_query = f'''
	INSERT INTO raw_data.ticket_study_time (id
,created_at
,in_date_time
,out_date_time
,ticket_id
,updated_at)
	VALUES (%s, %s, %s, %s, %s, %s)
'''

ticket_wallet_insert_query = f'''
	INSERT INTO raw_data.ticket_wallet (id
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
,user_id)
	VALUES (, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''


ticket_wallet_payment_insert_query = f'''
	INSERT INTO raw_data.ticket_wallet_payment (id
,is_success
,created_at
,payment_block_id
,ticket_wallet_id
,updated_at)
	VALUES (%s, %s, %s, %s, %s, %s)
'''

time_insert_query = f'''
	INSERT INTO raw_data.time (id
,month
,week
,minute
,paymentitemid)
	VALUES (%s, %s, %s, %s, %s)
'''

university_insert_query = f'''
	INSERT INTO raw_data.university (id
,name
,email_suffix
,region
,type
,created_at
,updated_at
,old_id)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''


university_information_insert_query = f'''
	INSERT INTO raw_data.university_information (id
,department
,major
,division_id
,university_id
,seoltab_pass
,created_at
,updated_at)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

user_insert_query = f'''
	INSERT INTO raw_data.user (id
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
,latest_login_at)
	VALUES (, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

user_address_insert_query = f'''
	INSERT INTO raw_data.user_address (id
,user_id
,post_code
,address1
,address2
,created_at
,updated_at)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

user_contract_insert_query = f'''
	INSERT INTO raw_data.user_contract (id
,createdat
,updatedat
,deletedat
,userid
,paidat
,isconfirmed
,paymentid
,deviceid
,contractid)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

user_device_insert_query = f'''
	INSERT INTO raw_data.user_device (id
,user_id
,device
,token
,created_at
,updated_at)
	VALUES (%s, %s, %s, %s, %s, %s)
'''

user_protector_insert_query = f'''
	INSERT INTO raw_data.user_protector (id
,user_id
,name
,phone_number)
	VALUES (%s, %s, %s, %s)
'''

user_push_insert_query = f'''
	INSERT INTO raw_data.user_push (id
,user_id
,type
,is_opt_in
,opt_in_date)
	VALUES (%s, %s, %s, %s, %s)
'''

user_term_agreement_insert_query = f'''
	INSERT INTO raw_data.user_term_agreement (id
,user_id
,term_id
,agreement_date)
	VALUES (%s, %s, %s, %s)
'''

user_time_line_insert_query = f'''
	INSERT INTO raw_data.user_time_line (id
,is_teacher
,version
,created_at
,updated_at
,user_id
,available_time_ids
,desired_time_ids)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

user_using_time_line_insert_query = f'''
	INSERT INTO raw_data.user_using_time_line (id
,created_at
,lecture_id
,updated_at
,user_id
,user_time_line_id
,time_ids)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

vbank_insert_query = f'''
	INSERT INTO raw_data.vbank (id
,createdat
,name
,code
,accountnumber
,duedate
,iscancelled
,paymentid)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''





