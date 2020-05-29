

select 
    SRC:"ApprovalStatus"::varchar(30) as APPROVAL_STATUS,
    SRC:"Department Head"::varchar(80) as DEPARTMENT_HEAD,  
    SRC:"Department Name"::varchar(30) as DEPARTMENT_NAME,
    SRC:"Parent Department"::varchar(30) as PARENT_DEPARTMENT,
    SRC:"createdTime"::varchar(15) as CREATED_TIME_INT,    
    SRC:"modifiedTime"::varchar(15) as MODIFIED_TIME_INT,  
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as MODIFIED_TIME,
    SRC:"ownerID"::varchar(30) as OWNER_ID,    
    SRC:"ownerName"::varchar(80) as OWNER_NAME,
    SRC:"recordId"::varchar(20) as RECORDID

from ZOHO_OA_EDW.RAW_ZOHO.TB_HIST_P_DEPARTMENTVIEW   

