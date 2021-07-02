{{
    config (
        alias ='TB_Weekly_TimeSheets_Status',
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'Record_id'
        )
}}

with Generate_Rownum as
(
select *,ROW_NUMBER() OVER(ORDER BY NULL) as ID,DENSE_RANK() OVER(order by Record_Captured_at asc ) as record_Captured_rank
from {{source('open_air','TB_HIST_WEEKLY_TIMESHEETS_STATUS')}}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where Record_captured_at > (select max(Record_captured_at) from {{ this }})

{% endif %}

order by ID
),

Generate_WeekStarting_Date as
(
select *,right(trim(SPLIT(first_value(CURRENT_WEEK) over(partition by record_Captured_rank order by ID asc),'-')[0]),10) as Week_starting
from Generate_Rownum
),

GetRecentData as
(
    select * from Generate_WeekStarting_Date
    qualify rank() over(partition by Week_starting order by Record_Captured_at desc ) = 1
    order by ID
),


Current_Status_Derivation as
(
    SELECT 
    ID,
    Employee,
    trim(SPLIT(Employee,',')[1]) as Employee_first_name,
    Department,
    Manager,
    Employee_type,
    Employee_id,
    Employee_location,
    Previous_week,
    Current_week,
    case  
        when Current_week like '%A%' then 'Approved'
        when Current_week like '%S%' then 'Submitted'
        when Current_week = 'X' then 'Missing'
        when Current_week like '%R%' then 'Rejected'
        when Current_week like '%O%' then 'Open'
        when Current_week = 'N' then 'Not in company during reporting period'
        else 'Others' 
        end as Timesheets_status,
    case  
        when Previous_week like '%A%' then 'Approved'
        when Previous_week like '%S%' then 'Submitted'
        when Previous_week = 'X' then 'Missing'
        when Previous_week like '%R%' then 'Rejected'
        when Previous_week like '%O%' then 'Open'
        when Previous_week = 'N' then 'Not in company during reporting period'
        else 'Others' 
        end as PrevWeek_Timesheets_status,
    Week_starting,
    Record_captured_at
    from GetRecentData
    WHERE (Previous_week not like '%/%' and Employee != '%Generated%' and trim(Employee) != '')
    ORDER BY ID
),

Form_Record_id as
(
    SELECT md5(cast(
    coalesce(cast(Employee as varchar
), '') || '-' || coalesce(cast(Week_starting as varchar
), '')
 as varchar
)) as Record_id,* from Current_Status_Derivation
),


Data_type_change as (
    SELECT 
    Record_id,
    Employee::varchar(150) as Employee,
    Employee_first_name::varchar(50) as Employee_first_name,
    Week_starting::date as Week_starting,
    Department::varchar(100) as Department,
    Manager::varchar(100) as Manager,
    Employee_type::varchar(50) as Employee_type,
    Employee_id::varchar(50) as Employee_id,
    Employee_location::varchar(100) as Employee_location,
    Previous_week::varchar(100) as Previous_week_timesheets_status,
    PrevWeek_Timesheets_status::varchar(50) as Previous_week_Timesheets_status_description,
    Current_week::varchar(100) as Week_starting_Timesheets_status,
    Timesheets_status::varchar(75) as Week_starting_Timesheets_status_description,
    Record_captured_at
    from Form_Record_id
)

select * from Data_type_change