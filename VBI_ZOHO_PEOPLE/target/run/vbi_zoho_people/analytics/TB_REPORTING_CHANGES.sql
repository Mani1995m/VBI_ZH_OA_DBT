

      create or replace transient table ZOHO_OA_EDW.ANALYTICS.TB_REPORTING_CHANGES  as
      (

select 
    SRC:"modifiedTime"::varchar(15) as MODIFIED_TIME_INT,
    SRC:"Associate ID"::varchar(80) as ASSOCIATE_ID,
    SRC:"Comments"::varchar(1000) as COMMENTS,
    SRC:"New People Manager"::varchar(80) as NEW_PEOPLE_MANAGER,
    SRC:"Added By"::varchar(80) as ADDED_BY,
    SRC:"ownerID"::varchar(30) as OWNER_ID,
    SRC:"ApprovalStatus"::varchar(30) as APPROVAL_STATUS,
    SRC:"Effective"::Date as EFFECTIVE,
    SRC:"recordId"::varchar(20) as RECORDID,
    SRC:"Modified By"::varchar(80) as MODIFIED_BY,
    SRC:"Current People Manager"::varchar(80) as CURRENT_PEOPLE_MANAGER,
    TO_TIMESTAMP((SRC:"Modified time")::String, 'DD-mon-YYYY HH:MI:SS') as MODIFIED_TIME,
    SRC:"ownerName"::varchar(80) as OWNER_NAME,
    SRC:"createdTime"::varchar(15) as CREATED_TIME_INT 
    
from ZOHO_OA_EDW.RAW_ZOHO.TB_HIST_REPORTING_CHANGES_VIEW


      );
    