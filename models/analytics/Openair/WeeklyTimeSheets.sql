{{
    config (
        alias = 'WeeklyTimeSheets',
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
    Week_starting as WeekStarting,
    All_hours as AllHours,
    Submitted_hours as SubmittedHours,
    Rejected_hours as RejectedHours,
    Approved_hours as ApprovedHours
from {{ ref('stg_oa_weeklytimesheets') }}
)


Select * from NeededColumns
