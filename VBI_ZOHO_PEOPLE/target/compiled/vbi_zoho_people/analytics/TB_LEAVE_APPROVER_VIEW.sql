

select 
    SRC:"ApprovalStatus"::String as APPROVAL_STATUS,
    SRC:"Associate ID"::String as ASSOCIATE_ID,
    SRC:"Explain Reason for leave"::String as EXPLAIN_REASON_FOR_LEAVE,  
    SRC:"From (mm/dd/yyyy)"::Date as FROM_DATE,  
    SRC:"Leave type"::String as LEAVE_TYPE,
    SRC:"To (mm/dd/yyyy)"::Date as TO_DATE,
    SRC:"createdTime"::String as CREATED_TIME_INT,    
    SRC:"modifiedTime"::String as MODIFIED_TIME_INT,  
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as MODIFIED_TIME,
    SRC:"ownerID"::String as OWNER_ID,    
    SRC:"ownerName"::String as OWNER_NAME,
    SRC:"recordId"::String as RECORDID

from ZOHO_OA_EDW.RAW_ZOHO.TB_HIST_LEAVE_APPROVER_VIEW   


    where MODIFIED_TIME > ( select max(MODIFIED_TIME) from ZOHO_OA_EDW.ANALYTICS.TB_LEAVE_APPROVER_VIEW )
