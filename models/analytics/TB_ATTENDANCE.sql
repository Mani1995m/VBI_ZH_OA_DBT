{{
    config(
        materialized = 'incremental',
        unique_key = 'recordid' 
    )
}}

Select 
    SRC:"ApprovalStatus"::varchar(30) as approval_status,
    to_date(SRC:"Attendance Day"::String , 'DD-Mon-YYYY') as attendance_day,
    TRY_TO_TIME(SRC:"Break Time"::String) as break_time,    
    SRC:"Description"::varchar(250) as description,
    TRY_TO_TIME(SRC:"Deviation Time"::String) as deviation_time,
    SRC:"Employee ID"::varchar(80) as employee_id,
    case
        when length(SRC:"Expected FromTime")=0 then
            to_timestamp('0001-01-01 00:00:00')
        else
            to_timestamp(SRC:"Expected FromTime"::String, 'DD-MON-YYYY HH:MI:SS')
    end
    as expected_fromtime,
    case
        when length(SRC:"Expected ToTime")=0 then
            to_timestamp('0001-01-01 00:00:00')
        else
            to_timestamp(SRC:"Expected ToTime"::String, 'DD-MON-YYYY HH:MI:SS')
    end
    as expected_totime,
    case
        when length(SRC:"From Time")=0 then
            to_timestamp('0001-01-01 00:00:00')
        else
            to_timestamp(SRC:"From Time"::String, 'DD-MON-YYYY HH:MI:SS')
    end
    as from_time,
    try_cast(SRC:"InputType"::String as Smallint) as input_type,
    SRC:"Over Time"::varchar(50) as over_time,  
    try_cast(SRC:"Status"::String as Smallint) as status,    
    case
        when length(SRC:"To Time")=0 then
            to_timestamp('0001-01-01 00:00:00')
        else
            to_timestamp(SRC:"To Time"::String, 'DD-MON-YYYY HH:MI:SS')
    end
    as to_time,
    SRC:"Total Hours"::varchar(50)  as total_hours,
    SRC:"Working Hours"::varchar(50)  as working_hours,
    SRC:"createdTime"::varchar(15) as created_time_int, 
    SRC:"modifiedTime"::varchar(15) as modified_time_int,  
    case
        when length(MODIFIED_TIME_INT)<1 then
            try_to_timestamp('')
        else
            to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3)
    end
    as modified_time,
    SRC:"ownerID"::varchar(30) as owner_id,    
    SRC:"ownerName"::varchar(80) as owner_name,
    SRC:"recordId"::varchar(20) as recordid
    
from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_P_ATTENDANCEFORM_VIEW')}}

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}