{{
    config (
        alias ='Int_Weekly_TimeSheets',
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'Record_id'
        )
}}


With getdata as 
(
    select * from {{source('open_air','Weekly_TimeSheets')}}

    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where Record_captured_at > (select max(Record_captured_at) from {{ this }})

    {% endif %}

),


Data_type_change as (
    SELECT 
    Department::varchar as Department,
    Client::varchar as Client,
    Project::varchar as Project,
    Employee::varchar as Employee,
    Week_starting::date as Week_starting,
    All_hours::number(38,2) as All_hours,
    Submitted_hours::number(38,2) as Submitted_hours,
    Rejected_hours::number(38,2) as Rejected_hours,
    Approved_hours::number(38,2) as Approved_hours
    from getdata
),
    
Final as (
SELECT {{ dbt_utils.surrogate_key('Department','Client','Project','Employee','Week_starting') }} as Record_id,
    Department,
    Client,
    Project,
    Employee,
    Week_starting,
    All_hours,
    Submitted_hours,
    Rejected_hours,
    Approved_hours
from Data_type_change
)

select * from Final