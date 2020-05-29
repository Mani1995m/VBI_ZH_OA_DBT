{{
    config(
        materialized = 'incremental',
        unique_key = 'RECORDID'
    )
}}

select 
    SRC:"ApprovalStatus"::varchar(30) as APPROVAL_STATUS,
    SRC:"Department Head"::varchar(80) as DEPARTMENT_HEAD,  
    SRC:"Department Name"::varchar(30) as DEPARTMENT_NAME,
    SRC:"Parent Department"::varchar(30) as PARENT_DEPARTMENT,
    SRC:"createdTime"::varchar(15) as CREATED_TIME_INT,    
    SRC:"modifiedTime"::varchar(15) as MODIFIED_TIME_INT,  
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as MODIFIED_TIME,
    SRC:"ownerID"::varchar(30) as OWNER_ID,    
    SRC:"ownerName"::varchar(80) as OWNER_NAME,
    SRC:"recordId"::varchar(20) as RECORDID

from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_P_DEPARTMENTVIEW')}}   

{% if is_incremental() %}
    where MODIFIED_TIME > ( select max(MODIFIED_TIME) from {{ this }} )
{% endif %}