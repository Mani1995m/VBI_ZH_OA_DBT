{{
    config(
        materialized = 'incremental',
        unique_key = 'RECORDID'
    )
}}

select 
    SRC:"Added By"::varchar(80) as ADDED_BY,
    TO_TIMESTAMP((SRC:"Added time")::String, 'DD-mon-YYYY HH:MI:SS') as ADDED_TIME,
    SRC:"ApprovalStatus"::varchar(30) as APPROVAL_STATUS,
    SRC:"Designation Name"::varchar(80) as DESIGNATION_NAME,
    SRC:"Mail Alias"::varchar(40) as MAIL_ALIAS,  
    SRC:"Modified By"::varchar(80) as MODIFIED_BY,
    SRC:"createdTime"::varchar(15) as CREATED_TIME_INT,    
    SRC:"modifiedTime"::varchar(15) as MODIFIED_TIME_INT,  
    TO_TIMESTAMP((SRC:"Modified time")::String, 'DD-mon-YYYY HH:MI:SS') as MODIFIED_TIME,
    SRC:"ownerID"::varchar(30) as OWNER_ID,    
    SRC:"ownerName"::varchar(80) as OWNER_NAME,
    SRC:"recordId"::varchar(20) as RECORDID

from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_P_DESIGNATIONVIEW')}}   

{% if is_incremental() %}
    where MODIFIED_TIME > ( select max(MODIFIED_TIME) from {{ this }} )
{% endif %}