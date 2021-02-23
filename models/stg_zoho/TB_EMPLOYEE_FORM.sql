{{
    config(
        materialized = 'incremental',
        unique_key = 'record_id'
    )
}}



select
    record_id,
    SRC:"AboutMe"::varchar(250) as about_me,
    SRC:"Add_Attachements"::varchar(150) as add_attachments,
    SRC:"Add_Attachements_downloadUrl"::varchar(250) as add_attachments_download_url,
    split_part(SRC:"AddedBy",'-', 1)::varchar(20) as added_by,
    SRC:"AddedBy.ID"::varchar(25) as addedby_id,
    SRC:"AddedTime"::varchar(15) as added_time_int,
    //TO_TIMESTAMP((SRC:"AddedTime")::String, 'DD-mon-YYYY HH:MI:SS') as added_time,
    SRC:"ApprovalStatus"::varchar(25) as approval_status,
    SRC:"Associate_Status"::varchar(25) as associate_status,
    SRC:"CreatedTime"::varchar(15) as created_time_int,
    SRC:"Current_Job_Description"::varchar(250) as current_job_description,
    //SRC:"Date_of_End_of_Internship",
    case 
        when length(SRC:"Date_of_End_of_Internship") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Date_of_End_of_Internship"::String , 'DD-Mon-YYYY')
    end
    as date_of_end_of_internship,
    //SRC:"Date_of_Start_of_Internship",
    case 
        when length(SRC:"Date_of_Start_of_Internship") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Date_of_Start_of_Internship"::String , 'DD-Mon-YYYY')
    end
    as date_of_start_of_internship,
    
    //SRC:"Dateofexit",
    case 
        when length(SRC:"Dateofexit") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Dateofexit"::String , 'DD-Mon-YYYY')
    end
    as date_of_exit,
    
    //SRC:"Dateofjoining",
    case 
        when length(SRC:"Dateofjoining") = 0 then
            to_date('01-Jan-0001', 'DD-Mon-YYYY')
        else
            to_date(SRC:"Dateofjoining"::String , 'DD-Mon-YYYY')
    end
    as date_of_joining,


    SRC:"Department"::varchar(30) as department,
    SRC:"Department.ID"::varchar(25) as department_id,
    SRC:"Designation"::varchar(80) as designation,
    SRC:"Designation.ID"::varchar(25) as designation_id,
    SRC:"EmailID"::varchar(40) as email_id,
    SRC:"Employee_type"::varchar(30) as employee_type,
    SRC:"EmployeeID"::varchar(15) as employee_id,
    SRC:"Employeestatus"::varchar(25) as employee_status,
    SRC:"Exp_in_VBI_Months"::smallint as exp_in_vbi_months,
    SRC:"Experience"::smallint as experience_in_years,
    SRC:"Experience.displayValue"::varchar(30) as experience_displayvalue,
    SRC:"Experience_in_VBI"::varchar(10) as experience_in_vbi,
    SRC:"Experience1"::varchar(10) as experience_1,
    SRC:"Expertise"::varchar(500) as expertise,
    SRC:"Extension"::varchar(10) as extension,
    SRC:"FirstName"::varchar(50) as first_name,
    SRC:"LastName"::varchar(50) as last_name,
    SRC:"LocationName"::varchar(30) as location_name,
    SRC:"LocationName.ID"::varchar(25) as location_name_id,
    //SRC:"ModifiedBy" as modifiedby,
    split_part(SRC:"ModifiedBy",'-', 1)::varchar(20) as modified_by,
    SRC:"ModifiedBy.ID"::varchar(25) as modifiedby_id,
    SRC:"ModifiedTime"::varchar(15) as modified_time_int,
    //TO_TIMESTAMP((SRC:"ModifiedTime")::String, 'DD-mon-YYYY HH:MI:SS') as modified_time,
    SRC:"Nick_Name"::varchar(50) as first_name,
    SRC:"Original_DOJ_for_transfers",
    SRC:"Photo"::varchar(150) as photo,
    SRC:"Photo_downloadUrl"::varchar(150) as photo_download_url,
    SRC:"Project"::varchar(80) as project,
    SRC:"Project.ID"::varchar(25) as project_id,
    SRC:"Reporting_To" as reporting_to,
    SRC:"Reporting_To.ID"::varchar(25) as reportingto_id,
    SRC:"Reporting_To.MailID"::varchar(40) as reportingto_emailid,
    //SRC:"tabularSections",
    SRC:"Work_location"::varchar(30) as work_location,
    SRC:"Work_phone"::varchar(25) as work_phone,
    SRC:"Zoho_ID"::varchar(25) as zoho_id,
    SRC:"ZUID"::varchar(25) as zuid


from {{ source('ZOHO_PEOPLE_VIEW', 'TB_HIST_EMPLOYEE')}} 

-- {% if is_incremental() %}
--     where modified_time > ( select max(modified_time) from {{ this }} )
-- {% endif %}