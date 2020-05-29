

Select 
    SRC:"ApprovalStatus"::String as APPROVALSTATUS,
    SRC:"Date of joining (dd-MMM-yyyy)"::Date as DATE_OF_JOINING,
    SRC:"Department"::String as DEPARTMENT,
    SRC:"Email ID"::String as EMAIL_ID,
    SRC:"Employee Status"::String as EMPLOYEE_STATUS,
    SRC:"EmployeeID"::String as EMPLOYEE_ID,
    SRC:"First Name"::String as FIRST_NAME,
    SRC:"Last Name"::String as LAST_NAME,
    SRC:"Reporting To"::String as REPORTING_TO,
    SRC:"Title"::String as TITLE,
    SRC:"createdTime"::String as CREATED_TIME_INT,
    SRC:"modifiedTime"::String as MODIFIED_TIME_INT,
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as MODIFIED_TIME,
    SRC:"ownerID"::String as OWNERID,
    SRC:"ownerName"::String as OWNERNAME,
    SRC:"recordId"::String as RECORDID

from ZOHO_OA_EDW.RAW_ZOHO.TB_HIST_EMPLOYEEINACTIVEVIEW

