{{
    config (
        alias ='TB_Weekly_TimeSheets_Status'
        )
}}

with SubQuery as 
(
    SELECT $1 as Employee,
         $2 as Department,
     $3 as Manager,         
     $4 as Employee_type,
     $5 as Employee_id,
     $6 as Employee_location,
     $7 as Previous_week,
     $8 as Current_week,
     right(trim(SPLIT( first_VALUE($8) OVER(
        ORDER BY METADATA$FILENAME
    ),'-')[0]),10) Week_starting,
    convert_timezone('Asia/Calcutta', current_timestamp(2))::timestamp_ntz as Record_captured_at
     FROM @STAGE_OA_BLOB/vbidw/Openair/1__Weekly_Timesheet_Status_Report__Jay_report.csv (file_format => FF_CSV_OA)
),


Remove_null as 
(
    SELECT * 
    from SubQuery WHERE (Previous_week not like '%/%' and Previous_week !='' and Previous_week is not null)
),

Final as 
(
    SELECT {{ dbt_utils.surrogate_key('Employee','Week_starting') }} as Record_id,* from Remove_null
)

SELECT * FROM Final