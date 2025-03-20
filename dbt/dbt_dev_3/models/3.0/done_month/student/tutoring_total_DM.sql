{{ config(
    materialized='incremental',
    unique_key='id',  
    on_schema_change='append_new_columns'
) }}

WITH source_data AS (
    SELECT l.*, concat(user_id, "subject") as user_subject FROM {{ ref('lecture_DM') }} l
),

update_existing AS (
    SELECT 
        id,user_id, lecture_id, "subject", lecture_status, lecture_done_month, user_subject
    FROM source_data
    WHERE lecture_status in ('ACTIVE','EMPTY_SCHEDULE','MATCHING_TEACHER','REGISTER')
),

insert_new AS (
    SELECT 
        (SELECT COALESCE(MAX(id), 0) FROM {{ this }}) + ROW_NUMBER() OVER() AS id,  
        user_id, lecture_id, "subject", lecture_status, lecture_done_month, user_subject
    FROM source_data s
    WHERE lecture_status in ('ACTIVE','EMPTY_SCHEDULE','MATCHING_TEACHER','REGISTER')
    AND NOT EXISTS (
        SELECT 1 
        FROM {{ this }} t
        WHERE t.user_subject = s.user_subject
        AND t.lecture_status in ('ACTIVE','EMPTY_SCHEDULE','MATCHING_TEACHER','REGISTER')
    )
)

SELECT * FROM update_existing

{% if is_incremental() %}
UNION ALL 
SELECT * FROM insert_new
{% endif %}
