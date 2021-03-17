{{
    config(
        materialized = 'incremental',
        unique_key = 'record_id'
    )
}}

select
    record_id,
    IFNULL(SRC:"AboutMe"::varchar(1500), NULL) as about_me,
    IFNULL(SRC:"Add_Attachements"::varchar(150), NULL) as add_attachments,
    IFNULL(SRC:"Add_Attachements_downloadUrl"::varchar(250), NULL) as add_attachments_download_url,
    split_part(SRC:"AddedBy",'-', 1)::varchar(20) as added_by,
    SRC:"AddedBy.ID"::varchar(25) as addedby_id,
    SRC:"AddedTime"::varchar(15) as added_time_int,
    TO_TIMESTAMP((SRC:"AddedTime")::String, 'DD-mon-YYYY HH:MI:SS') as added_time,
    SRC:"ApprovalStatus"::varchar(25) as approval_status,
    IFNULL(SRC:"Associate_Status"::varchar(25), NULL) as associate_status,
    SRC:"CreatedTime"::varchar(15) as created_time_int,
    IFNULL(SRC:"Current_Job_Description"::varchar(250), NULL) as current_job_description,
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


    IFNULL(SRC:"Department"::varchar(30), NULL) as department,
    IFNULL(SRC:"Department.ID"::varchar(25), NULL) as department_id,
    SRC:"Designation"::varchar(80) as designation,
    IFNULL(SRC:"Designation.ID"::varchar(25), NULL) as designation_id,
    IFNULL(SRC:"EmailID"::varchar(40), NULL) as email_id,
    IFNULL(SRC:"Employee_type"::varchar(30), NULL) as employee_type,
    IFNULL(SRC:"EmployeeID"::varchar(15), NULL) as employee_id,
    IFNULL(SRC:"Employeestatus"::varchar(25), NULL) as employee_status,
    IFNULL(SRC:"Exp_in_VBI_Months"::smallint, NULL) as exp_in_vbi_months,
    IFNULL(SRC:"Experience"::smallint, NULL) as experience_in_years,
    IFNULL(SRC:"Experience.displayValue"::varchar(30), NULL) as experience_displayvalue,
    IFNULL(SRC:"Experience_in_VBI"::varchar(10), NULL) as experience_in_vbi,
    IFNULL(SRC:"Experience1"::varchar(10), NULL) as experience_1,
    IFNULL(SRC:"Expertise"::varchar(500), NULL) as expertise,
    IFNULL(SRC:"Extension"::varchar(10), NULL) as extension,
    IFNULL(SRC:"FirstName"::varchar(50), NULL) as first_name,
    IFNULL(SRC:"LastName"::varchar(50), NULL) as last_name,
    IFNULL(SRC:"LocationName"::varchar(30), NULL) as location_name,
    IFNULL(SRC:"LocationName.ID"::varchar(25), NULL) as location_name_id,
    //SRC:"ModifiedBy" as modifiedby,
    split_part(SRC:"ModifiedBy",'-', 1)::varchar(20) as modified_by,
    SRC:"ModifiedBy.ID"::varchar(25) as modifiedby_id,
    SRC:"ModifiedTime"::varchar(15) as modified_time_int,    
    TO_TIMESTAMP((SRC:"ModifiedTime")::String) as modified_time,
    IFNULL(SRC:"Nick_Name"::varchar(50), NULL) as nick_name,
    IFNULL(SRC:"Original_DOJ_for_transfers", NULL) as original_doj_for_transfers,
    IFNULL(SRC:"Photo"::varchar(150), NULL) as photo,
    IFNULL(SRC:"Photo_downloadUrl"::varchar(150), NULL) as photo_download_url,
    IFNULL(SRC:"Project"::varchar(80), NULL) as project,
    IFNULL(SRC:"Project.ID"::varchar(25), NULL) as project_id,
    IFNULL(SRC:"Reporting_To", NULL) as reporting_to,
    IFNULL(SRC:"Reporting_To.ID"::varchar(25), NULL) as reportingto_id,
    IFNULL(SRC:"Reporting_To.MailID"::varchar(40), NULL) as reportingto_emailid,
    //SRC:"tabularSections",
    IFNULL(SRC:"Work_location"::varchar(30), NULL) as work_location,
    IFNULL(SRC:"Work_phone"::varchar(25), NULL) as work_phone,
    SRC:"Zoho_ID"::varchar(25) as zoho_id,
    IFNULL(SRC:"ZUID"::varchar(25), NULL) as zuid

from {{ source('ZOHO_PEOPLE_FORM', 'TB_HIST_EMPLOYEE')}} 

{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}