

Select 
    SRC:"ApprovalStatus"::varchar(30) as APPROVALSTATUS,
    case 
        when length(SRC:"Date of joining (dd-MMM-yyyy)") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Date of joining (dd-MMM-yyyy)"::String , 'DD-Mon-YYYY')
    end
    as DATE_OF_JOINING,
    SRC:"Department"::varchar(30) as DEPARTMENT,
    SRC:"Email ID"::varchar(40) as EMAIL_ID,
    SRC:"Employee Status"::varchar(25) as EMPLOYEE_STATUS,
    SRC:"EmployeeID"::varchar(15) as EMPLOYEE_ID,
    SRC:"First Name"::varchar(50) as FIRST_NAME,
    SRC:"Last Name"::varchar(50) as LAST_NAME,
    SRC:"Reporting To"::varchar(80) as REPORTING_TO,
    SRC:"Title"::varchar(80) as TITLE,
    SRC:"createdTime"::varchar(15) as CREATED_TIME_INT,
    SRC:"modifiedTime"::varchar(15) as MODIFIED_TIME_INT,
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as MODIFIED_TIME,
    SRC:"ownerID"::varchar(30) as OWNERID,
    SRC:"ownerName"::varchar(80) as OWNERNAME,
    SRC:"recordId"::varchar(20) as RECORDID

from ZOHO_OA_EDW.RAW_ZOHO.TB_HIST_EMPLOYEEINACTIVEVIEW

