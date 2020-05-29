{{
    config(
        materialized = 'incremental',
        unique_key = 'RECORDID' 
    )
}}

select
    SRC:"Added By"::varchar(80) as ADDED_BY,
    TO_TIMESTAMP((SRC:"Added time")::String, 'DD-mon-YYYY HH:MI:SS') as ADDED_TIME,
    SRC:"ApprovalStatus"::String as APPROVALSTATUS,
    case
        when SRC:"ApprovalTime" not like '%AM%' and SRC:"ApprovalTime" not like '%PM%' then 
            TO_TIMESTAMP((SRC:"ApprovalTime")::String,  'DD-MON-YYYY HH:MI:SS') 
        else
            TO_TIMESTAMP((SRC:"ApprovalTime")::String,  'DD-MON-YYYY HH12:MI AM')
     end
     as APPROVED_TIME,
    SRC:"Comments for Cancellation by Admin"::varchar(1000) as COMMENTS_FOR_CANCELLATION_BY_ADMIN,
    case
        when length(SRC:"DOR") = 0 then
           to_date('01-Jan-0001', 'DD-MON-YYYY')
        else
           to_date(SRC:"DOR"::String, 'DD-MON-YYYY')
    end 
    as DATE_OF_RESIGNATION,
    case
        when length(SRC:"Date - Time for WFH") = 0 then
           to_timestamp('01-Jan-0001', 'DD-MON-YYYY')
        else
           to_timestamp(SRC:"Date - Time for WFH"::String, 'DD-MON-YYYY HH:MI:SS')
    end 
    as DATE_TIME_FOR_WFH,
    TRY_CAST(SRC:"Date of request"::String as Date) as DATE_OF_REQUEST,
    TRY_CAST(SRC:"Days Taken"::String as NUMBER(4,1)) as DAYS_TAKEN,
    SRC:"Employee ID"::varchar(80) as EMPLOYEE_ID,
    TRY_CAST(SRC:"Friday flag"::String  as Smallint) as FRIDAY_FLAG,
    SRC:"From"::Date as FROM_DATE,
    TRY_CAST(SRC:"Late Flag"::String  as Smallint) as LATE_FLAG,
    SRC:"Leave Type"::varchar(40) as LEAVE_TYPE,
    SRC:"Medical certificate"::varchar(150) as MEDICAL_CERTIFICATE,
    SRC:"Medical certificate_downloadUrl"::varchar(150) as MEDICAL_CERTIFICATE_DOWNLOAD_URL,
    SRC:"Modified By"::varchar(80) as MODIFIED_BY,
    TO_TIMESTAMP((SRC:"Modified time")::String, 'DD-mon-YYYY HH:MI:SS') as MODIFIED_TIME,
    TRY_CAST(SRC:"NP DOR Flag"::String  as Smallint) as NOTICE_PERIOD,
    TRY_CAST(SRC:"Number of Days"::String  as Smallint) as NO_OF_DAYS,
    SRC:"Reason for Leave"::varchar(1000) as REASON_FOR_LEAVE_1,
    SRC:"Reason for leave"::varchar(1000) as REASON_FOR_LEAVE_2,
    SRC:"To"::Date as TO_DATE,
    SRC:"Was it a late Application (EL to be reduced)"::varchar(5) as WAS_IT_A_LATE_APPLICATION,
    SRC:"createdTime"::varchar(15) as CREATED_TIME_INT,
    SRC:"modifiedTime"::varchar(15) as MODIFIED_TIME_INT,
    SRC:"ownerID"::varchar(30) as OWNERID,
    SRC:"ownerName"::varchar(80) as OWNERNAME,
    SRC:"recordId"::varchar(20) as RECORDID
 
from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_P_APPLYLEAVEVIEW')}}

{% if is_incremental() %}
    where MODIFIED_TIME > ( select max(MODIFIED_TIME) from {{ this }} )
{% endif %}