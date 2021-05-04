{{
    config(
        materialized = 'incremental',
        unique_key = 'record_id'
    )
}}

select
    record_id,
    IFNULL(SRC:"AboutMe"::varchar, NULL) as about_me,
    IFNULL(SRC:"Add_Attachements"::varchar, NULL) as add_attachments,
    IFNULL(SRC:"Add_Attachements_downloadUrl"::varchar, NULL) as add_attachments_download_url,
    split_part(SRC:"AddedBy",'-', 1)::varchar as added_by,
    SRC:"AddedBy.ID"::varchar as addedby_id,
    SRC:"AddedTime"::varchar as added_time_int,
    TO_TIMESTAMP((SRC:"AddedTime")::String, 'DD-mon-YYYY HH:MI:SS') as added_time,
    SRC:"ApprovalStatus"::varchar as approval_status,
    IFNULL(SRC:"Associate_Status"::varchar, NULL) as associate_status,
    SRC:"CreatedTime"::varchar as created_time_int,
    IFNULL(SRC:"Current_Job_Description"::varchar, NULL) as current_job_description,
    -- SRC:"Date_of_End_of_Internship",
    case 
        when length(SRC:"Date_of_End_of_Internship") = 0 then
            NULL
            -- to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Date_of_End_of_Internship"::String , 'DD-Mon-YYYY')
    end
    as date_of_end_of_internship,
    -- SRC:"Date_of_Start_of_Internship",
    case 
        when length(SRC:"Date_of_Start_of_Internship") = 0 then
            NULL
            -- to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Date_of_Start_of_Internship"::String , 'DD-Mon-YYYY')
    end
    as date_of_start_of_internship,
    IFNULL(SRC:"Employee_type"::varchar, NULL) as employee_type,
    -- SRC:"Dateofexit",
    case 
        when employee_type = 'Intern' then
            date_of_end_of_internship
        when length(SRC:"Dateofexit") = 0 then 
            NULL
            -- to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Dateofexit"::String , 'DD-Mon-YYYY')
    end
    as date_of_exit,
    
    -- SRC:"Dateofjoining",
    case 
        when employee_type = 'Intern' then
            date_of_start_of_internship
        when length(SRC:"Dateofjoining") = 0 then
            NULL
            -- to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Dateofjoining"::String , 'DD-Mon-YYYY')
    end
    as date_of_joining,


    IFNULL(SRC:"Department"::varchar, NULL) as department,
    IFNULL(SRC:"Department.ID"::varchar, NULL) as department_id,
    SRC:"Designation"::varchar as designation,
    IFNULL(SRC:"Designation.ID"::varchar, NULL) as designation_id,
    IFNULL(SRC:"EmailID"::varchar, NULL) as email_id,
    
    IFNULL(SRC:"EmployeeID"::varchar, NULL) as employee_id,
    IFNULL(SRC:"Employeestatus"::varchar, NULL) as employee_status,
    IFNULL(SRC:"Exp_in_VBI_Months"::smallint, NULL) as exp_in_vbi_months,
    IFNULL(SRC:"Experience"::smallint, NULL) as experience_in_years,
    IFNULL(SRC:"Experience.displayValue"::varchar, NULL) as experience_displayvalue,
    IFNULL(SRC:"Experience_in_VBI"::varchar, NULL) as experience_in_vbi,
    IFNULL(SRC:"Experience1"::varchar, NULL) as experience_1,
    IFNULL(SRC:"Expertise"::varchar, NULL) as expertise,
    IFNULL(SRC:"Extension"::varchar, NULL) as extension,
    IFNULL(SRC:"FirstName"::varchar, NULL) as first_name,
    IFNULL(SRC:"LastName"::varchar, NULL) as last_name,
    IFNULL(SRC:"LocationName"::varchar, NULL) as location_name,
    IFNULL(SRC:"LocationName.ID"::varchar, NULL) as location_name_id,
    //SRC:"ModifiedBy" as modifiedby,
    split_part(SRC:"ModifiedBy",'-', 1)::varchar as modified_by,
    SRC:"ModifiedBy.ID"::varchar as modifiedby_id,
    SRC:"ModifiedTime"::varchar as modified_time_int,    
    TO_TIMESTAMP((SRC:"ModifiedTime")::String) as modified_time,
    IFNULL(SRC:"Nick_Name"::varchar, NULL) as nick_name,
    IFNULL(SRC:"Original_DOJ_for_transfers", NULL) as original_doj_for_transfers,
    IFNULL(SRC:"Photo"::varchar, NULL) as photo,
    IFNULL(SRC:"Photo_downloadUrl"::varchar, NULL) as photo_download_url,
    IFNULL(SRC:"Project"::varchar, NULL) as project,
    IFNULL(SRC:"Project.ID"::varchar, NULL) as project_id,
    IFNULL(SRC:"Reporting_To", NULL) as reporting_to,
    IFNULL(SRC:"Reporting_To.ID"::varchar, NULL) as reportingto_id,
    IFNULL(SRC:"Reporting_To.MailID"::varchar, NULL) as reportingto_emailid,
    //SRC:"tabularSections",
    IFNULL(SRC:"Work_location"::varchar, NULL) as work_location,
    IFNULL(SRC:"Work_phone"::varchar, NULL) as work_phone,
    SRC:"Zoho_ID"::varchar as zoho_id,
    IFNULL(SRC:"ZUID"::varchar, NULL) as zuid

from {{ source('ZOHO_PEOPLE_FORM', 'TB_HIST_EMPLOYEE')}} 

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}