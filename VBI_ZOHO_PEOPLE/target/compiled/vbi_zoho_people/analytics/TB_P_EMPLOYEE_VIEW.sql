

Select 
    SRC:"Added By"::String as ADDED_BY,
    TO_TIMESTAMP((SRC:"Added time")::String, 'DD-mon-YYYY HH:MI:SS') as ADDED_TIME,
    SRC:"ApprovalStatus"::String as APPROVALSTATUS,
    SRC:"Date of joining"::Date as DATE_OF_JOINING,
    SRC:"Department"::String as DEPARTMENT,
    SRC:"Email ID"::String as EMAIL_ID,
    SRC:"Employee Status"::String as EMPLOYEE_STATUS,
    SRC:"Employee Type"::String as EMPLOYEE_TYPE,
    SRC:"EmployeeID"::String as EMPLOYEE_ID,
    SRC:"First Name"::String as FIRST_NAME,
    SRC:"Last Name"::String as LAST_NAME,
    SRC:"Modified By"::String as MODIFIED_BY,
    TO_TIMESTAMP((SRC:"Modified time")::String, 'DD-mon-YYYY HH:MI:SS') as MODIFIED_TIME,
    SRC:"People Manager"::String as PEOPLE_MANAGER,
    SRC:"Photo"::String as PHOTO,
    SRC:"Photo_downloadUrl"::String as PHOTO_DOWNLOAD_URL,
    SRC:"Project"::String as PROJECT,
    SRC:"Title"::String as TITLE,
    SRC:"createdTime"::String as CREATED_TIME_INT,
    SRC:"modifiedTime"::String as MODIFIED_TIME_INT,
    SRC:"ownerID"::String as OWNER_ID,
    SRC:"ownerName"::String as OWNER_NAME,
    SRC:"recordId"::String as RECORDID
    
from ZOHO_OA_EDW.RAW_ZOHO.TB_HIST_P_EMPLOYEEVIEW

