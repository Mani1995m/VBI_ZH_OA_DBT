{{
    config(
        materialized = 'incremental',
        unique_key = 'recordid'
    )
}}

select 
    -- SRC:"Added By"::varchar(80) as added_by,
    split_part(SRC:"Added By",'-', 1)::varchar(20) as added_by,
    SRC:"ApprovalStatus"::varchar(30) as approval_status,
    -- SRC:"Associate ID"::varchar(80) as associate_id,
    split_part(SRC:"Associate ID", ' ', -1)::varchar(20) as employee_id,
    SRC:"Comments"::varchar(1000) as comments,
    SRC:"Current People Manager"::varchar(80) as current_people_manager,
    SRC:"Effective"::Date as effective,
    SRC:"New People Manager"::varchar(80) as new_people_manager,   
    -- SRC:"Modified By"::varchar(80) as modified_by,
    split_part(SRC:"Modified By",'-', 1)::varchar(20) as modified_by,
    SRC:"createdTime"::varchar(15) as created_time_int,
    SRC:"modifiedTime"::varchar(15) as modified_time_int,    
    TO_TIMESTAMP((SRC:"Modified time")::String, 'DD-mon-YYYY HH:MI:SS') as modified_time,
    SRC:"ownerID"::varchar(30) as owner_id,
    SRC:"ownerName"::varchar(80) as owner_name,    
    SRC:"recordId"::varchar(20) as recordid
    
from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_REPORTING_CHANGES_VIEW')}}

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}
