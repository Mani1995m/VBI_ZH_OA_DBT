{{
    config(
        materialized = 'incremental',
        unique_key = 'recordid' 
    )
}}

select
    -- SRC:"Added By"::varchar(80) as added_by,
    split_part(SRC:"Added By",'-', 1)::varchar(20) as added_by,
    TO_TIMESTAMP((SRC:"Added time")::String, 'DD-mon-YYYY HH:MI:SS') as added_time,
    SRC:"ApprovalStatus"::String as approvalstatus,
    case
        when SRC:"ApprovalTime" not like '%AM%' and SRC:"ApprovalTime" not like '%PM%' then 
            TO_TIMESTAMP((SRC:"ApprovalTime")::String,  'DD-MON-YYYY HH:MI:SS') 
        else
            TO_TIMESTAMP((SRC:"ApprovalTime")::String,  'DD-MON-YYYY HH12:MI AM')
     end
     as approved_time,
    SRC:"Comments for Cancellation by Admin"::varchar(1000) as comments_for_cancellation_by_admin,
    case
        when length(SRC:"DOR") = 0 then
           to_date('01-Jan-0001', 'DD-MON-YYYY')
        else
           to_date(SRC:"DOR"::String, 'DD-MON-YYYY')
    end 
    as date_of_resignation,
    case
        when length(SRC:"Date - Time for WFH") = 0 then
           to_timestamp('01-Jan-0001', 'DD-MON-YYYY')
        else
           to_timestamp(SRC:"Date - Time for WFH"   ::String, 'DD-MON-YYYY HH:MI:SS')
    end 
    as date_time_for_wfh,
    TRY_CAST(SRC:"Date of request"::String as Date) as date_of_request,
    TRY_CAST(SRC:"Days Taken"::String as NUMBER(4,1)) as days_taken,
    -- SRC:"Employee ID"::varchar(80) as employee_id,
    split_part(SRC:"Employee ID",'-', 1)::varchar(20) as employee_id,
    TRY_CAST(SRC:"Friday flag"::String  as Smallint) as friday_flag,
    SRC:"From"::Date as from_date,
    TRY_CAST(SRC:"Late Flag"::String  as Smallint) as late_flag,
    SRC:"Leave Type"::varchar(40) as leave_type,
    SRC:"Medical certificate"::varchar(150) as medical_certificate,
    SRC:"Medical certificate_downloadUrl"::varchar(150) as medical_certificate_download_url,
    -- SRC:"Modified By"::varchar(80) as modified_by,
    split_part(SRC:"Modified By",'-', 1)::varchar(20) as modified_by,
    TO_TIMESTAMP((SRC:"Modified time")::String, 'DD-mon-YYYY HH:MI:SS') as modified_time,
    TRY_CAST(SRC:"NP DOR Flag"::String  as Smallint) as notice_period,
    TRY_CAST(SRC:"Number of Days"::String  as Smallint) as no_of_days,
    SRC:"Reason for Leave"::varchar(1000) as reason_for_leave_1,
    SRC:"Reason for leave"::varchar(1000) as reason_for_leave_2,
    SRC:"To"::Date as to_date,
    SRC:"Was it a late Application (EL to be reduced)"::varchar(5) as was_it_a_late_application,
    SRC:"createdTime"::varchar(15) as created_time_int,
    SRC:"modifiedTime"::varchar(15) as modified_time_int,
    SRC:"ownerID"::varchar(30) as ownerid,
    SRC:"ownerName"::varchar(80) as ownername,
    SRC:"recordId"::varchar(20) as recordid
 
from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_P_APPLYLEAVEVIEW')}}

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}