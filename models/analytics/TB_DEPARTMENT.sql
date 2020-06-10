{{
    config(
        materialized = 'incremental',
        unique_key = 'recordid'
    )
}}

select 
    SRC:"ApprovalStatus"::varchar(30) as approval_status,
    SRC:"Department Head"::varchar(80) as department_head,  
    SRC:"Department Name"::varchar(30) as department_name,
    SRC:"Parent Department"::varchar(30) as parent_department,
    SRC:"createdTime"::varchar(15) as created_time_int,    
    SRC:"modifiedTime"::varchar(15) as modified_time_int,  
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as modified_time,
    SRC:"ownerID"::varchar(30) as owner_id,    
    SRC:"ownerName"::varchar(80) as owner_name,
    SRC:"recordId"::varchar(20) as recordid

from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_P_DEPARTMENTVIEW')}}   

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}