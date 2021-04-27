{{
    config (
        alias = 'WeeklyTimeSheets',
        materialized = 'view'
    )
}}

With NeededColumns as 
(
    SELECT 
    Department,
    Client,
    Project,
    Employee,
    Week_starting as WeekStarting,
    All_hours as AllHours,
    Submitted_hours as SubmittedHours,
    Rejected_hours as RejectedHours,
    Approved_hours as ApprovedHours
from {{ ref('stg_oa_weeklytimesheets') }}
),

Final as
(
    Select * from NeededColumns
)

Select * from Final
