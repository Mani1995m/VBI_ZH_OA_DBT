

select 
    SRC:"modifiedTime"::String as MODIFIED_TIME_INT,
    SRC:"Associate ID"::String as ASSOCIATE_ID,
    SRC:"Comments"::String as COMMENTS,
    SRC:"New People Manager"::String as NEW_PEOPLE_MANAGER,
    SRC:"Added By"::String as ADDED_BY,
    SRC:"ownerID"::String as OWNER_ID,
    SRC:"ApprovalStatus"::String as APPROVAL_STATUS,
    SRC:"Effective"::Date as EFFECTIVE,
    SRC:"recordId"::String as RECORDID,
    SRC:"Modified By"::String as MODIFIED_BY,
    SRC:"Current People Manager"::String as CURRENT_PEOPLE_MANAGER,
    TO_TIMESTAMP((SRC:"Modified time")::String, 'DD-mon-YYYY HH:MI:SS') as MODIFIED_TIME,
    SRC:"ownerName"::String as OWNER_NAME,
    SRC:"createdTime"::String as CREATED_TIME_INT    
    
from ZOHO_OA_EDW.RAW_ZOHO.TB_HIST_REPORTING_CHANGES_VIEW


    where MODIFIED_TIME > ( select max(MODIFIED_TIME) from ZOHO_OA_EDW.ANALYTICS.TB_REPORTING_CHANGES_VIEW )
