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
    SRC:"CreatedTime"::varchar(15) as created_time_int,
    TO_TIMESTAMP((SRC:"CreatedTime")::String) as created_time,
    split_part(SRC:"ModifiedBy",'-', 1)::varchar(20) as modified_by,
    SRC:"Emergency_Contact_Address"::varchar(200) as emergency_contact_address,
    SRC:"Emergency_contact_Person"::varchar(50) as emergency_contact_person,
    SRC:"Emergency_contact_number"::varchar(25) as emergency_contact_number,
    SRC:"ModifiedBy.ID"::varchar(25) as modifiedby_id,
    SRC:"ModifiedTime"::varchar(15) as modified_time_int,    
    TO_TIMESTAMP((SRC:"ModifiedTime")::String) as modified_time,
    SRC:"Zoho_ID"::varchar(25) as zoho_id

from {{ source('ZOHO_PEOPLE_FORM', 'TB_HIST_INTERCOMPANY_TRANSFER')}} 

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}