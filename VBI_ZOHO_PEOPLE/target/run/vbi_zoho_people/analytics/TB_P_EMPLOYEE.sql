

      create or replace transient table ZOHO_OA_EDW.ANALYTICS.TB_P_EMPLOYEE  as
      (

Select 
    SRC:"Added By"::varchar(80) as ADDED_BY,
    TO_TIMESTAMP((SRC:"Added time")::String, 'DD-mon-YYYY HH:MI:SS') as ADDED_TIME,
    SRC:"ApprovalStatus"::varchar(30) as APPROVALSTATUS,
    case 
        when length(SRC:"Date of joining") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Date of joining"::String , 'DD-Mon-YYYY')
    end
    as DATE_OF_JOINING,
    SRC:"Department"::varchar(30) as DEPARTMENT,
    SRC:"Email ID"::varchar(40) as EMAIL_ID,
    SRC:"Employee Status"::varchar(25) as EMPLOYEE_STATUS,
    SRC:"Employee Type"::varchar(30) as EMPLOYEE_TYPE,
    SRC:"EmployeeID"::varchar(15) as EMPLOYEE_ID,
    SRC:"First Name"::varchar(50) as FIRST_NAME,
    SRC:"Last Name"::varchar(50) as LAST_NAME,
    SRC:"Modified By"::varchar(80) as MODIFIED_BY,
    TO_TIMESTAMP((SRC:"Modified time")::String, 'DD-mon-YYYY HH:MI:SS') as MODIFIED_TIME,
    SRC:"People Manager"::varchar(80) as PEOPLE_MANAGER,
    SRC:"Photo"::varchar(150) as PHOTO,
    SRC:"Photo_downloadUrl"::varchar(150) as PHOTO_DOWNLOAD_URL,
    SRC:"Project"::varchar(80) as PROJECT,
    SRC:"Title"::varchar(80) as TITLE,
    SRC:"createdTime"::varchar(15) as CREATED_TIME_INT,
    SRC:"modifiedTime"::varchar(15) as MODIFIED_TIME_INT,
    SRC:"ownerID"::varchar(30) as OWNER_ID,
    SRC:"ownerName"::varchar(80) as OWNER_NAME,
    SRC:"recordId"::varchar(20) as RECORDID
    
from ZOHO_OA_EDW.RAW_ZOHO.TB_HIST_P_EMPLOYEEVIEW


      );
    