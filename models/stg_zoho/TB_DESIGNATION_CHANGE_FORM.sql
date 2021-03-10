{{
    config(
        materialized = 'incremental',
        unique_key = 'record_id'
    )
}}

select
    record_id,
    SRC:"ApprovalStatus"::varchar(25) as approval_status,
    split_part(SRC:"Associate_ID", ' ', -1)::varchar(20) as employee,
    SRC:"Associate_ID.ID"::varchar(20) as employee_id,
    SRC:"Comments"::varchar(200) as Comments,
    SRC:"CreatedTime"::varchar(15) as created_time_int,
    TO_TIMESTAMP((SRC:"CreatedTime")::String) as created_time,
    SRC:"Current_Designation"::varchar(200) as current_designation,
    SRC:"Current_Designation.ID"::varchar(25) as current_designation_id,
       case 
        when length(SRC:"Effective_From") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Effective_From"::String , 'DD-Mon-YYYY')
    end
    as effective_From, 
    SRC:"ModifiedTime"::varchar(15) as modified_time_int,    
    TO_TIMESTAMP((SRC:"ModifiedTime")::String) as modified_time,
    SRC:"New_Designation"::varchar(200) as new_designation,
    SRC:"New_Designation.ID"::varchar(25) as new_designation_id,
    SRC:"Zoho_ID"::varchar(25) as zoho_id

from {{ source('ZOHO_PEOPLE_FORM', 'TB_HIST_DESIGNATION_CHANGE_REQUEST')}} 

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}