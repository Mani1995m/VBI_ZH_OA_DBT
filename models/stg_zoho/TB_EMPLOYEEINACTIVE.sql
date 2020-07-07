{{
    config(
        materialized = 'incremental',
        unique_key = 'recordid'
    )
}}

Select 
    SRC:"ApprovalStatus"::varchar(30) as approvalstatus,
    case 
        when length(SRC:"Date of joining (dd-MMM-yyyy)") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Date of joining (dd-MMM-yyyy)"::String , 'DD-Mon-YYYY')
    end
    as date_of_joining,
    SRC:"Department"::varchar(30) as department,
    SRC:"Email ID"::varchar(40) as email_id,
    SRC:"Employee Status"::varchar(25) as employee_status,
    SRC:"EmployeeID"::varchar(15) as employee_id,
    SRC:"First Name"::varchar(50) as first_name,
    SRC:"Last Name"::varchar(50) as last_name,
    SRC:"Reporting To"::varchar(80) as reporting_to,
    SRC:"Title"::varchar(80) as designation,
    SRC:"createdTime"::varchar(15) as created_time_int,
    SRC:"modifiedTime"::varchar(15) as modified_time_int,
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as modified_time,
    SRC:"ownerID"::varchar(30) as ownerid,
    SRC:"ownerName"::varchar(80) as ownername,
    SRC:"recordId"::varchar(20) as recordid

from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_EMPLOYEEINACTIVEVIEW')}}

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}