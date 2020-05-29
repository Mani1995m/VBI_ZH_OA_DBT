

      create or replace transient table ZOHO_OA_EDW.ANALYTICS.TB_ASSOCIATE  as
      (

Select 
    SRC:"ApprovalStatus"::varchar(30) as APPROVALSTATUS,
    SRC:"Associate ID"::varchar(15) as ASSOCIATE_ID,
    SRC:"Associate type"::varchar(30) as ASSOCIATE_TYPE,
     case 
        when length(SRC:"Date of joining (dd-MMM-yyyy)") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Date of joining (dd-MMM-yyyy)"::String , 'DD-Mon-YYYY')
    end
    as DATE_OF_JOINING,
    SRC:"Department"::varchar(30) as DEPARTMENT,
    SRC:"Designation"::varchar(80) as DESIGNATION,
    SRC:"Email ID"::varchar(40) as EMAIL_ID,
    SRC:"First Name"::varchar(50) as FIRST_NAME,
    SRC:"Last Name"::varchar(50) as LAST_NAME,
    SRC:"Location"::varchar(40) as LOCATION,
    SRC:"People Manager"::varchar(80) as PEOPLE_MANAGER,
    SRC:"Photo"::varchar(150) as PHOTO,
    SRC:"Photo_downloadUrl"::varchar(150) as PHOTO_DOWNLOAD_URL,
    SRC:"Work phone"::varchar(25) as WORK_PHONE,
    SRC:"createdTime"::varchar(15) as CREATED_TIME_INT, 
    SRC:"modifiedTime"::varchar(15) as MODIFIED_TIME_INT,  
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as MODIFIED_TIME,
    SRC:"ownerID"::varchar(30) as OWNER_ID,    
    SRC:"ownerName"::varchar(80) as OWNER_NAME,
    SRC:"recordId"::varchar(20) as RECORDID
    
from ZOHO_OA_EDW.RAW_ZOHO.TB_HIST_ASSOCIATE_VIEW


      );
    