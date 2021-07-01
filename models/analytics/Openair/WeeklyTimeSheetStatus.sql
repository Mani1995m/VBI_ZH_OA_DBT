{{
    config (
        alias = 'WeeklyTimeSheetStatus',
        transient = false
    )
}}

With WeeklyTimeSheetStatus as 
(
    SELECT * from {{ ref('stg_oa_weeklytimesheetstatus') }}
),

FinalColumns as 
(
SELECT 
    Employee,
    Employee_first_name as EmployeeFirstName,
    Week_starting as WeekStarting,
    Department,
    Manager,
    Employee_type as EmployeeType,
    Employee_id as EmployeeEmail,
    Employee_location as EmployeeLocation,
    Previous_week_Timesheets_status_description as PreviousWeekTimesheetsStatus,
    Week_starting_Timesheets_status_description as WeekStartingTimesheetsStatus
    from WeeklyTimeSheetStatus
)

select * from FinalColumns