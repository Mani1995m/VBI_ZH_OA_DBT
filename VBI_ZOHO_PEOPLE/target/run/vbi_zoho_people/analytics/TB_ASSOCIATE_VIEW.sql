

      create or replace transient table ZOHO_OA_EDW.ANALYTICS.TB_ASSOCIATE_VIEW  as
      (

Select 
    SRC:"ApprovalStatus"::String as APPROVALSTATUS,
    SRC:"Associate ID"::String as ASSOCIATE_ID,
    SRC:"Associate type"::String as ASSOCIATE_TYPE,
    SRC:"Date of joining (dd-MMM-yyyy)"::Date as DATE_OF_JOINING,
    SRC:"Department"::String as DEPARTMENT,
    SRC:"Designation"::String as DESIGNATION,
    SRC:"Email ID"::String as EMAIL_ID,
    SRC:"First Name"::String as FIRST_NAME,
    SRC:"Last Name"::String as LAST_NAME,
    SRC:"Location"::String as LOCATION,
    SRC:"People Manager"::String as PEOPLE_MANAGER,
    SRC:"Photo"::String as PHOTO,
    SRC:"Photo_downloadUrl"::String as PHOTO_DOWNLOAD_URL,
    SRC:"Work phone"::String as WORK_PHONE,
    SRC:"createdTime"::String as CREATED_TIME_INT, 
    SRC:"modifiedTime"::String as MODIFIED_TIME_INT,  
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as MODIFIED_TIME,
    SRC:"ownerID"::String as OWNER_ID,    
    SRC:"ownerName"::String as OWNER_NAME,
    SRC:"recordId"::String as RECORDID
    
from ZOHO_OA_EDW.RAW_ZOHO.TB_HIST_ASSOCIATE_VIEW


      );
    