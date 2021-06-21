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
        else
            to_date(SRC:"Date_of_End_of_Internship"::String , 'DD-Mon-YYYY')
    end
    as date_of_end_of_internship,
    -- SRC:"Date_of_Start_of_Internship",
    case 
        when length(SRC:"Date_of_Start_of_Internship") = 0 then
            NULL
        else
            to_date(SRC:"Date_of_Start_of_Internship"::String , 'DD-Mon-YYYY')
    end
    as date_of_start_of_internship,
        case 
        when length(SRC:"Date_of_birth") = 0 then
            NULL
        else
            to_date(SRC:"Date_of_birth"::String , 'DD-Mon-YYYY')
    end
    as date_of_birth,
    IFNULL(SRC:"Employee_type"::varchar, NULL) as employee_type,
    -- SRC:"Dateofexit",
    case 
        when employee_type = 'Intern' then
            date_of_end_of_internship
        when length(SRC:"Dateofexit") = 0 then 
            NULL
        else
            to_date(SRC:"Dateofexit"::String , 'DD-Mon-YYYY')
    end
    as date_of_exit,
    

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
    IFNULL(SRC:"Mobile"::varchar, NULL) as mobile,
    split_part(SRC:"ModifiedBy",'-', 1)::varchar as modified_by,
    SRC:"ModifiedBy.ID"::varchar as modifiedby_id,
    SRC:"ModifiedTime"::varchar as modified_time_int,    
    TO_TIMESTAMP((SRC:"ModifiedTime")::String) as modified_time,
    IFNULL(SRC:"Nick_Name"::varchar, NULL) as nick_name,
    case 
        when length(SRC:"Original_DOJ_for_transfers") = 0 then
            NULL
        else
            to_date(SRC:"Original_DOJ_for_transfers"::String , 'DD-Mon-YYYY')
    end
    as original_doj_for_transfers,
    SRC:"Dateofjoining" as recorded_doj,
    case 
        when employee_type in ('Intern','On Contract') then
            date_of_start_of_internship
        when original_doj_for_transfers is NOT NULL then
            original_doj_for_transfers
        when length(SRC:"Dateofjoining") = 0 then 
            NULL
        else
            to_date(SRC:"Dateofjoining"::String , 'DD-Mon-YYYY')
    end
    as date_of_joining,
    case 
        when employee_type = 'Intern' then
            0
        when employee_status = 'Active' then
            datediff(month, date_of_joining, current_date)
        else
            datediff(month, date_of_joining, date_of_exit)
    end
    as vbi_exp_months,
    to_number(vbi_exp_months/12,5, 1) as vbi_exp_years,
    -- pass experience detail
    Past_Exp_Days,
    Past_Exp_Months,
    Past_Exp_years,
    (vbi_exp_years + coalesce(Past_Exp_years,0) ) as past_and_vbi_experience,
    IFNULL(SRC:"Photo"::varchar, NULL) as photo,
    IFNULL(SRC:"Photo_downloadUrl"::varchar, NULL) as photo_download_url,
    IFNULL(SRC:"Project"::varchar, NULL) as project,
    IFNULL(SRC:"Project.ID"::varchar, NULL) as project_id,
    IFNULL(split_part(SRC:"Reporting_To",'VBI', 1)::varchar, NULL) as reporting_to,
    IFNULL(SRC:"Reporting_To.ID"::varchar, NULL) as reportingto_id,
    IFNULL(SRC:"Reporting_To.MailID"::varchar, NULL) as reportingto_emailid,
    -- SRC:"tabularSections",
    IFNULL(SRC:"Work_location"::varchar, NULL) as work_location,
    IFNULL(SRC:"Work_phone"::varchar, NULL) as work_phone,
    SRC:"Zoho_ID"::varchar as zoho_id,
    IFNULL(SRC:"ZUID"::varchar, NULL) as zuid

from 
    {{ source('ZOHO_PEOPLE_FORM', 'TB_HIST_EMPLOYEE')}} x
    LEFT OUTER JOIN
    (
        select 
            employee_id, 
            sum(Past_Exp_days) as Past_Exp_days,
            sum(Past_Exp_Months) as Past_Exp_Months,
            sum(Past_Exp_Years) as Past_Exp_years 
        from
            (
                Select 
                    IFNULL(SRC:"EmployeeID"::varchar(15), NULL) as employee_id,
                    case 
                        when length(b.value:"FromDate") = 0 then
                            NULL
                        else
                            to_date(b.value:"FromDate"::String , 'DD-Mon-YYYY')
                    end
                    as Past_Exp_From_Date,
                    case 
                        when length(b.value:"Todate") = 0 then
                            NULL
                        else
                            to_date(b.value:"Todate"::String , 'DD-Mon-YYYY')
                    end
                    as Past_Exp_To_Date,
                    datediff(day, Past_Exp_From_Date, Past_Exp_To_Date) as Past_Exp_days,
                    datediff(month, Past_Exp_From_Date, Past_Exp_To_Date) as Past_Exp_months,
                    to_number(Past_Exp_months/12,5, 1) as Past_Exp_years
                from 
                    {{ source('ZOHO_PEOPLE_FORM', 'TB_HIST_EMPLOYEE')}} a,
                    lateral flatten (input=> a.SRC:"tabularSections":"Work experience")b)
                group by employee_id
    )
    y
    on x.SRC:"EmployeeID" = y.employee_id


{% if is_incremental() %}
    where modified_time > ( select max(modified_time) from {{ this }} )
{% endif %}