{{
    config(
        materialized = 'incremental',
        unique_key = 'record_id'
    )
}}

select
    record_id,
    split_part(SRC:"AddedBy",'-', 1)::varchar(20) as added_by,
    SRC:"AddedBy.ID"::varchar(25) as addedby_id,
    SRC:"AddedTime"::varchar(15) as added_time_int,
    TO_TIMESTAMP((SRC:"AddedTime")::String, 'DD-mon-YYYY HH:MI:SS') as added_time,
    SRC:"ApprovalStatus"::varchar(25) as approval_status,
    SRC:"ApprovalTime"::varchar(15) as approval_time_int,
    TO_TIMESTAMP((SRC:"ApprovalTime")::String, 'DD-mon-YYYY HH12:MI AM') as approval_time,
    split_part(SRC:"Associate_ID", ' ', -1)::varchar(20) as employee,
    SRC:"Associate_ID.ID"::varchar(20) as employee_id,
    SRC:"CreatedTime"::varchar(15) as created_time_int,
    TO_TIMESTAMP((SRC:"CreatedTime")::String) as created_time,
    SRC:"Current_Proj_Department"::varchar(25) as current_proj_department,
    SRC:"Current_Proj_Department.ID"::varchar(25) as current_proj_department_id,
    SRC:"Current_Project_Department"::varchar(25) as current_project_department,
    SRC:"Current_Project_Department.ID"::varchar(25) as current_project_department_id,
    SRC:"Current_Project_Lead"::varchar(25) as current_project_lead,
    SRC:"Current_Project_Lead.ID"::varchar(25) as current_project_lead_id,
    SRC:"Description"::varchar(25) as description,
    case 
        when length(SRC:"Effective_Date") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Effective_Date"::String , 'DD-Mon-YYYY')
    end
    as Effective_Date, 
    case 
        when length(SRC:"Effective_Date_Project_Lead_Change") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Effective_Date_Project_Lead_Change"::String , 'DD-Mon-YYYY')
    end
    as Effective_Date_Project_Lead_Change, 
    case 
        when length(SRC:"Effective_New_Project") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Effective_New_Project"::String , 'DD-Mon-YYYY')
    end
    as Effective_New_Project, 
    SRC:"Initiate_Post_Project_Feedback"::varchar(25) as Initiate_Post_Project_Feedback,
    SRC:"Mail_Alias"::varchar(25) as Mail_Alias,
    split_part(SRC:"ModifiedBy",'-', 1)::varchar(20) as modified_by,
    SRC:"ModifiedBy.ID"::varchar(25) as modifiedby_id,
    SRC:"ModifiedTime"::varchar(15) as modified_time_int,    
    TO_TIMESTAMP((SRC:"ModifiedTime")::String) as modified_time,
    SRC:"New_Proj_Department"::varchar(25) as new_proj_department,
    SRC:"New_Proj_Department.ID"::varchar(25) as new_proj_department_id,
    SRC:"New_Project_Department"::varchar(25) as new_project_department,
    SRC:"New_Project_Department.ID"::varchar(25) as new_project_department_id,
    SRC:"New_Project_Lead"::varchar(80) as new_project_lead,
    SRC:"New_Project_Lead.ID"::varchar(25) as new_project_lead_id,
    SRC:"Post_Project_Feedback_to_be_filled_by"::varchar(80) as Post_Project_Feedback_to_be_filled_by,
    SRC:"Post_Project_Feedback_to_be_filled_by.ID"::varchar(25) as Post_Project_Feedback_to_be_filled_by_id,
    SRC:"Proj_Department"::varchar(25) as Proj_Department,
    SRC:"Proj_Department.ID"::varchar(25) as Proj_Department_ID,
    SRC:"Project_Department"::varchar(25) as Project_Department,
    SRC:"Project_Department.ID"::varchar(25) as Project_Department_ID,
    SRC:"Project_Lead_New_Project"::varchar(80) as Project_Lead_New_Project,
    SRC:"Project_Lead_New_Project.ID"::varchar(25) as Project_Lead_New_Project_ID,
    SRC:"Reasons_for_Department_Change"::varchar(250) as Reasons_for_Department_Change,
    SRC:"Zoho_ID"::varchar(25) as zoho_id

from {{ source('ZOHO_PEOPLE_FORM', 'TB_HIST_PROJECT_DEPARTMENT_CHANGES')}} 

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}