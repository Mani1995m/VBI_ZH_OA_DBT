{{
    config (
        alias ='Int_Weekly_TimeSheets_Status',
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ('Employee','Department','Manager','EmployeeID','WeekStarting')
        )
}}


With getdata as 
(
    select * from {{source('open_air','Weekly_TimeSheets_Status')}}

    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where Record_captured_at > (select max(Record_captured_at) from {{ this }})

    {% endif %}

),


Data_type_change as (
    SELECT 
    Employee::varchar as Employee,
    Department::varchar as Department,
    Manager::varchar as Manager,
    EmployeeType::varchar as EmployeeType,
    EmployeeID::nvarchar as EmployeeID,
    EmployeeLocation::nvarchar as EmployeeLocation,
    PreviousWeek::varchar as PreviousWeek,
    CurrentWeek::varchar as CurrentWeek,
    CurrentStatus::varchar as CurrentStatus,
    WeekStarting::date as WeekStarting
    from getdata
),
    
Final as (
SELECT 
    Employee,
    Department,
    Manager,
    EmployeeType,
    EmployeeID,
    EmployeeLocation,
    PreviousWeek,
    CurrentWeek,
    WeekStarting
from Data_type_change
)

select * from Final