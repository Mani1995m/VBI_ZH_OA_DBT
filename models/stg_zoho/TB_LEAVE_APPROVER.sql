{{
    config(
        materialized = 'incremental',
        unique_key = 'recordid'
    )
}}

select 
    SRC:"ApprovalStatus"::varchar(30) as approval_status,
    -- SRC:"Associate ID"::varchar(80) as associate_id,
    split_part(SRC:"Associate ID",'-', 1)::varchar(20) as employee_id,
    SRC:"Explain Reason for leave"::varchar(1000) as explain_reason_for_leave,  
    SRC:"From (mm/dd/yyyy)"::Date as from_date,  
    SRC:"Leave type"::varchar(40) as leave_type,
    SRC:"To (mm/dd/yyyy)"::Date as to_date,
    SRC:"createdTime"::varchar(15) as created_time_int,    
    SRC:"modifiedTime"::varchar(15) as modified_time_int,  
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as modified_time,
    SRC:"ownerID"::varchar(30) as owner_id,    
    SRC:"ownerName"::varchar(80) as owner_name,
    SRC:"recordId"::varchar(20) as recordid

from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_LEAVE_APPROVER_VIEW')}}   

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}
