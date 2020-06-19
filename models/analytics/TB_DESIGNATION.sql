{{
    config(
        materialized = 'incremental',
        unique_key = 'recordid'
    )
}}

select 
    -- SRC:"Added By"::varchar(80) as added_by,
    split_part(SRC:"Added By",'-', 1)::varchar(20) as added_by,
    TO_TIMESTAMP((SRC:"Added time")::String, 'DD-mon-YYYY HH:MI:SS') as added_time,
    SRC:"ApprovalStatus"::varchar(30) as approval_status,
    SRC:"Designation Name"::varchar(80) as designation_name,
    SRC:"Mail Alias"::varchar(40) as mail_alias,  
    -- SRC:"Modified By"::varchar(80) as modified_by,
    split_part(SRC:"Modified By",'-', 1)::varchar(20) as modified_by,
    SRC:"createdTime"::varchar(15) as created_time_int,    
    SRC:"modifiedTime"::varchar(15) as modified_time_int,  
    TO_TIMESTAMP((SRC:"Modified time")::String, 'DD-mon-YYYY HH:MI:SS') as modified_time,
    SRC:"ownerID"::varchar(30) as owner_id,    
    SRC:"ownerName"::varchar(80) as owner_name,
    SRC:"recordId"::varchar(20) as recordid

from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_P_DESIGNATIONVIEW')}}   

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}