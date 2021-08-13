{{
    config (
        alias = 'MonthlyTimeSheets',
        transient = false
    )
}}

With NeededColumns as 
(
    SELECT 
    Department,
    Client,
    Project,
    Employee,
    Employee_First_Name as  EmployeeFirstName,
    Employee_email as EmployeeEmail,
    Timesheet_month as TimesheetMonth,
    All_hours as AllHours,
    Submitted_hours as SubmittedHours,
    Rejected_hours as RejectedHours,
    Approved_hours as ApprovedHours
from {{ ref('stg_oa_monthlytimesheets') }}
)


Select * from NeededColumns
