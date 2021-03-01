{{
    config(
        materialized = 'incremental',
        unique_key = 'record_id'
    )
}}

select
    record_id,
    split_part(SRC:"AddedBy",'-', 1)::varchar(20) as added_by,
    SRC:"AddedBy.ID"::varchar(25) as addedby_id,
    SRC:"AddedTime"::varchar(15) as added_time_int,
    TO_TIMESTAMP((SRC:"AddedTime")::String, 'DD-mon-YYYY HH:MI:SS') as added_time,
    SRC:"ApprovalStatus"::varchar(25) as approval_status,
    SRC:"ApprovalTime"::varchar(15) as approval_time_int,
    TO_TIMESTAMP((SRC:"ApprovalTime")::String, 'DD-mon-YYYY HH12:MI AM') as approval_time,
    split_part(SRC:"Associate_ID", ' ', -1)::varchar(20) as employee,
    SRC:"Associate_ID.ID"::varchar(20) as employee_id,
    SRC:"Comments"::varchar(200) as Comments,
    SRC:"CreatedTime"::varchar(15) as created_time_int,
    TO_TIMESTAMP((SRC:"CreatedTime")::String) as created_time,
    split_part(SRC:"Currently_Reporting_to", ' ', -1)::varchar(20)  as Currently_Reporting_to,
    SRC:"Currently_Reporting_to.ID"::varchar(25) as Currently_Reporting_to_id,
    case 
        when length(SRC:"Effective") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Effective"::String , 'DD-Mon-YYYY')
    end
    as Effective, 
    split_part(SRC:"ModifiedBy",'-', 1)::varchar(20) as modified_by,
    SRC:"ModifiedBy.ID"::varchar(25) as modifiedby_id,
    SRC:"ModifiedTime"::varchar(15) as modified_time_int,    
    TO_TIMESTAMP((SRC:"ModifiedTime")::String) as modified_time,
    split_part(SRC:"Will_be_reporting_to", ' ', -1)::varchar(20)  as Will_be_reporting_to,
    SRC:"Will_be_reporting_to.ID"::varchar(25) as Will_be_reporting_to_id,
    SRC:"Zoho_ID"::varchar(25) as zoho_id

from {{ source('ZOHO_PEOPLE_FORM', 'TB_HIST_REPORTING_CHANGES')}} 

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}