

Select 
    SRC:"ApprovalStatus"::varchar(30) as APPROVAL_STATUS,
    to_date(SRC:"Attendance Day"::String , 'DD-Mon-YYYY') as ATTENDANCE_DAY,
    TRY_TO_TIME(SRC:"Break Time"::String) as BREAK_TIME,    
    SRC:"Description"::varchar(250) as DESCRIPTION,
    TRY_TO_TIME(SRC:"Deviation Time"::String) as DEVIATION_TIME,
    SRC:"Employee ID"::varchar(80) as EMPLOYEE_ID,
    to_timestamp(SRC:"Expected FromTime"::String, 'DD-MON-YYYY HH:MI:SS') as EXPECTED_FROMTIME,
    to_timestamp(SRC:"Expected ToTime"::String, 'DD-MON-YYYY HH:MI:SS') as EXPECTED_TOTIME,
    case
        when length(SRC:"From Time")=0 then
            to_timestamp('0001-01-01 00:00:00')
        else
            to_timestamp(SRC:"From Time"::String, 'DD-MON-YYYY HH:MI:SS')
    end
    as FROM_TIME,
    try_cast(SRC:"InputType"::String as Smallint) as INPUT_TYPE,
    TRY_TO_TIME(SRC:"Over Time"::String) as OVER_TIME,  
    try_cast(SRC:"Status"::String as Smallint) as STATUS,    
    case
        when length(SRC:"To Time")=0 then
            to_timestamp('0001-01-01 00:00:00')
        else
            to_timestamp(SRC:"To Time"::String, 'DD-MON-YYYY HH:MI:SS')
    end
    as TO_TIME,
    to_time(SRC:"Total Hours"::String, 'HH24:MI') as TOTAL_HOURS,
    to_time(SRC:"Working Hours"::String, 'HH24:MI') as WORKING_HOURS,
    SRC:"createdTime"::varchar(15) as CREATED_TIME_INT, 
    SRC:"modifiedTime"::varchar(15) as MODIFIED_TIME_INT,  
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as MODIFIED_TIME,
    SRC:"ownerID"::varchar(30) as OWNER_ID,    
    SRC:"ownerName"::varchar(80) as OWNER_NAME,
    SRC:"recordId"::varchar(20) as RECORDID
    
from ZOHO_OA_EDW.RAW_ZOHO.TB_HIST_P_ATTENDANCEFORM_VIEW

