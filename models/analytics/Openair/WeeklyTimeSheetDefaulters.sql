{{
    config (
        alias = 'WeeklyTimeSheetDefaulters',
        transient = false
    )
}}

With TimeSheetLessThanFortyHours as 
(
select Employee,Employee_First_Name, Week_starting,Employee_email,
       sum(Submitted_hours+Approved_hours) as EnteredHours,
       {{var("Weekly_timesheet_Threshold")}} - sum(Submitted_hours+Approved_hours) as MissedHours
       from {{ ref('stg_oa_weeklytimesheets') }}
group by 1,2,3,4
having sum(Submitted_hours+Approved_hours) < 40
),

TimeSheetMissingAndOpen as
(
select Employee,Employee_first_name, Week_starting,Employee_id as Employee_email, 0 as EnteredHours, 40 as MissedHours
from {{ ref('stg_oa_weeklytimesheetstatus')}}
where Week_starting_Timesheets_status_description in ('Missing','Open')
),

UnionTimeSheetDefaulters as
(
    select * from TimeSheetLessThanFortyHours
    Union
    select * from TimeSheetMissingAndOpen
),

Final as 
(
    Select Employee,
           Employee_first_name as EmployeeFirstName,
           Employee_email as EmployeeEmailId,
           Week_starting as WeekStarting,
           EnteredHours,
           MissedHours
           from UnionTimeSheetDefaulters
)

Select * from Final