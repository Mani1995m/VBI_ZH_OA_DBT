{{
    config(
        materialized = 'ephemeral'
    )
}}

Select
    SRC:"ApprovalStatus"::varchar(30) as approvalstatus,
    SRC:"Associate ID"::varchar(15) as associate_id,
    SRC:"Associate type"::varchar(30) as associate_type,
    case 
        when length(SRC:"Date of joining (dd-MMM-yyyy)") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Date of joining (dd-MMM-yyyy)"::String , 'DD-Mon-YYYY')
    end
    as date_of_joining,
    SRC:"Department"::varchar(30) as department,
    SRC:"Designation"::varchar(80) as designation,
    SRC:"Email ID"::varchar(40) as email_id,
    SRC:"First Name"::varchar(50) as first_name,
    SRC:"Last Name"::varchar(50) as last_name,
    SRC:"Location"::varchar(40) as location,
    SRC:"People Manager"::varchar(80) as people_manager,
    SRC:"Photo"::varchar(150) as photo,
    SRC:"Photo_downloadUrl"::varchar(150) as photo_download_url,
    SRC:"Work phone"::varchar(25) as work_phone,
    SRC:"createdTime"::varchar(15) as created_time_int, 
    SRC:"modifiedTime"::varchar(15) as modified_time_int,  
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as modified_time,
    SRC:"ownerID"::varchar(30) as owner_id,    
    SRC:"ownerName"::varchar(80) as owner_name,
    SRC:"recordId"::varchar(20) as recordid
    
from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_ASSOCIATE_VIEW')}} 