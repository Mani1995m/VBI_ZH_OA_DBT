{{
    config(
        materialized = 'incremental',
        unique_key = 'RECORDID' 
    )
}}

Select 
    SRC:"ApprovalStatus"::varchar(30) as APPROVAL_STATUS,
    SRC:"Associate ID"::varchar(80) as ASSOCIATE_ID,
    SRC:"Current Department"::varchar(30) as CURRENT_DEPARTMENT,
    SRC:"Current Project"::varchar(15) as CURRENT_PROJECT,
    SRC:"Current Project Lead"::varchar(80) as CURRENT_PROJECT_LEAD,
    SRC:"Department"::varchar(30) as DEPARTMENT,
    SRC:"Initiate Post Project Feedback "::varchar(5) as INITIAL_POST_PROJECT_FEEDBACK,
    SRC:"New Department"::varchar(30) as NEW_DEPARTMENT,
    SRC:"New Project"::varchar(15) as NEW_PROJECT,
    SRC:"New Project Department Name"::varchar(50) as NEW_PROJECT_DEPARTMENT_NAME,
    SRC:"New Project Lead"::varchar(80) as NEW_PROJECT_LEAD,
    SRC:"Post Project Feedback to be filled by"::varchar(80) as POST_PROJECT_FEEDBACK_TO_BE_FILLED_BY,
    SRC:"Project Lead"::varchar(80) as PROJECT_LEAD,
    SRC:"Project Name"::varchar(80) as PROJECT_NAME,
    SRC:"Reasons for Department Change"::varchar(250) as REASONS_FOR_DEPARTMENT_CHANGE,
    SRC:"createdTime"::varchar(15) as CREATED_TIME_INT, 
    SRC:"modifiedTime"::varchar(15) as MODIFIED_TIME_INT,  
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as MODIFIED_TIME,
    SRC:"ownerID"::varchar(30) as OWNER_ID,    
    SRC:"ownerName"::varchar(80) as OWNER_NAME,
    SRC:"recordId"::varchar(20) as RECORDID
    
from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_PROJECT_DEPARTMENT_CHANGES_VIEW')}}

{% if is_incremental() %}
    where MODIFIED_TIME > ( select max(MODIFIED_TIME) from {{ this }} )
{% endif %}