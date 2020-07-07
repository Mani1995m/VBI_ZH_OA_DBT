{{
    config(
        materialized = 'incremental',
        unique_key = 'recordid' 
    )
}}

Select 
    SRC:"ApprovalStatus"::varchar(30) as approval_status,
    -- SRC:"Associate ID"::varchar(80) as associate_id,
    split_part(SRC:"Associate ID", ' ', -1)::varchar(20) as employee_id,
    SRC:"Current Department"::varchar(30) as current_department,
    SRC:"Current Project"::varchar(15) as current_project,
    SRC:"Current Project Lead"::varchar(80) as current_project_lead,
    SRC:"Department"::varchar(30) as department,
    SRC:"Initiate Post Project Feedback "::varchar(5) as initial_post_project_feedback,
    SRC:"New Department"::varchar(30) as new_department,
    SRC:"New Project"::varchar(15) as new_project,
    SRC:"New Project Department Name"::varchar(50) as new_project_department_name,
    SRC:"New Project Lead"::varchar(80) as new_project_lead,
    SRC:"Post Project Feedback to be filled by"::varchar(80) as post_project_feedback_to_be_filled_by,
    SRC:"Project Lead"::varchar(80) as project_lead,
    SRC:"Project Name"::varchar(80) as project_name,
    SRC:"Reasons for Department Change"::varchar(250) as reasons_for_department_change,
    SRC:"createdTime"::varchar(15) as created_time_int, 
    SRC:"modifiedTime"::varchar(15) as modified_time_int,  
    to_timestamp_ntz(MODIFIED_TIME_INT::BIGINT,3) as modified_time,
    SRC:"ownerID"::varchar(30) as owner_id,    
    SRC:"ownerName"::varchar(80) as owner_name,
    SRC:"recordId"::varchar(20) as recordid
    
from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_PROJECT_DEPARTMENT_CHANGES_VIEW')}}

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}