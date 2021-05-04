{{
    config (
        alias = 'WeeklyTimeSheetDefaulters',
        materialized = 'view'
    )
}}

With WeeklyTimeSheets as 
(
    SELECT * from {{ ref('stg_oa_weeklytimesheets') }}
),

TimeSheetsLessthan as 
(
select Employee, Week_starting as WeekStaring,
       sum(Submitted_hours+Approved_hours) as EnteredHours,
       40 - sum(Submitted_hours+Approved_hours) as MissingHours
       from WeeklyTimeSheets
group by 1,2
having sum(Submitted_hours+Approved_hours) < 40
)

Select * from TimeSheetsLessthan
