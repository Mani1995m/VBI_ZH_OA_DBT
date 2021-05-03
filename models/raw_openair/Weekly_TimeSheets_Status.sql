{{
    config (
        alias ='Weekly_TimeSheets_Status',
        materialized = 'incremental'
        )
}}



with SubQuery as 
(
    SELECT $1 as Employee,
         $2 as Department,
     $3 as Manager,         
     $4 as EmployeeType,
     $5 as EmployeeID,
     $6 as EmployeeLocation,
     $7 as PreviousWeek,
     $8 as CurrentWeek,
     case when $8 = 'A' then 'Approved'
     when $8 = 'S' then 'Submitted'
     when $8 = 'X' then 'Missing'
     when $8 = 'R' then 'Rejected'
     when $8 = 'O' then 'Open'
     else 'Others'
     end as CurrentStatus,
     right(trim(SPLIT( first_VALUE($8) OVER(
        ORDER BY METADATA$FILENAME
    ),'-')[0]),10) WeekStarting,
    convert_timezone('Asia/Calcutta', current_timestamp(2))::timestamp_ntz as Record_captured_at
     FROM @STAGE_OA_TEST/Ingest_SPO/1__Weekly_Timesheet_Status_Report__Jay_report.csv (file_format => FF_CSV_OA)
),


Final as 
(
    SELECT * from SubQuery WHERE (PreviousWeek not like '%/%' and PreviousWeek !='' and PreviousWeek is not null)
)

SELECT * FROM Final