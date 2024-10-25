{{ config(
    materialized='ephemeral'
) }}


WITH active_list AS (
    select *
        from
            (select lvt.lecture_vt_No, lvt.student_user_No,lvt.tutoring_state,lvt.reactive_datetime,lvt.subject,lvt.total_subject_done_month,lvt.name,lvt.tteok_ham_type,lvt.grade,lvt.crda
                        ,dense_rank() over(partition by lvt.student_user_No order by p.fst_order_id asc) as rn, p.fst_pay_date
                from {{ ref('pg_lecture_list') }} lvt
                left join {{ ref('pg_fst_payment') }} p on lvt.lecture_vt_No = p.lecture_vt_No
            ) A
        where A.tutoring_state not in ('FINISH','AUTO_FINISH','DONE')
    )
SELECT *
    FROM active_list