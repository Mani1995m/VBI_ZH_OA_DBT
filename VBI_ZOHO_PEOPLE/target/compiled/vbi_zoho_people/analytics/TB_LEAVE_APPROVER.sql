

select 
     SRC:"ApprovalStatus"::varchar(30) as APPROVAL_STATUS,
    SRC:"Associate ID"::varchar(80) as ASSOCIATE_ID,
    SRC:"Explain Reason for leave"::varchar(1000) as EXPLAIN_REASON_FOR_LEAVE,  
    SRC:"From (mm/dd/yyyy)"::Date as FROM_DATE,  
    SRC:"Leave type"::varchar(40) as LEAVE_TYPE,
    SRC:"To (mm/dd/yyyy)"::Date as TO_DATE,
    SRC:"createdTime"::varchar(15) as CREATED_TIME_INT,    
    SRC:"modifiedTime"::varchar(15) as MODIFIED_TIME_INT,  
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as MODIFIED_TIME,
    SRC:"ownerID"::varchar(30) as OWNER_ID,    
    SRC:"ownerName"::varchar(80) as OWNER_NAME,
    SRC:"recordId"::varchar(20) as RECORDID

from ZOHO_OA_EDW.RAW_ZOHO.TB_HIST_LEAVE_APPROVER_VIEW   

