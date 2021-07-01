{{
    config (
        alias = 'TB_Weekly_TimeSheets',
        materialized = 'incremental',
        incremental_strategy = "delete+insert",
        unique_key = 'Record_id'
        )
}}

with Generate_Rownum as
(
select *,ROW_NUMBER() OVER(ORDER BY NULL) as ID,DENSE_RANK() OVER(order by Record_Captured_at asc ) as record_Captured_rank
from {{source('open_air','TB_HIST_WEEKLY_TIMESHEETS')}}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where Record_captured_at > (select max(Record_captured_at) from {{ this }})

{% endif %}

order by ID
),

Generate_WeekStarting_Date as
(
select *,right(trim(SPLIT(first_value(ALL_HOURS) over(partition by record_Captured_rank order by ID asc),'-')[0]),10) as Week_starting
from Generate_Rownum
),

GetRecentData as
(
    select * from Generate_WeekStarting_Date
    qualify rank() over(partition by Week_starting order by Record_Captured_at desc ) = 1
    order by ID
),

Flatten_1 as 
(
    SELECT ID,Hierarchy,
    case len(Hierarchy)-len(ltrim(Hierarchy))
     when 0 then 'Department'
     when 5 then 'Client'
     when 10 then 'Project'
     else 'Employee'
     end as Hierarchy_level,
     Employee_First_Name,
     Employee_Email,
     Week_starting,
     All_hours,
     Submitted_hours,
     Rejected_hours,
     Approved_hours,
     Record_Captured_at
     FROM GetRecentData
    where Hierarchy not like 'Department/%' and  trim(Hierarchy) !='' and Hierarchy is not null and Hierarchy not like '%Generated%'
    order by ID
),

Flatten_2 as (
       SELECT  ID,Hierarchy,Hierarchy_level,
       CASE WHEN Hierarchy_level = 'Department' then trim(Hierarchy) end as Department,
       CASE WHEN Hierarchy_level = 'Client' then trim(Hierarchy) end as Client,
       CASE WHEN Hierarchy_level = 'Project' then trim(Hierarchy) end as Project,
       CASE WHEN Hierarchy_level = 'Employee' then trim(Hierarchy) end as Employee,
       Employee_First_Name,
       Employee_Email,
       Week_starting,
       All_hours,
       Submitted_hours,
       Rejected_hours,
       Approved_hours,
       Record_Captured_at
       from Flatten_1
       order by ID
),

Fill_Nulls as
(
SELECT
ID,
case when Department is null then LAG(Department) ignore nulls OVER(ORDER BY ID) else Department end as Department,
case when Client is null then LAG(Client) ignore nulls OVER(ORDER BY ID) else Client end as Client,
case when Project is null then LAG(Project) ignore nulls OVER(ORDER BY ID) else Project end as Project,
Employee,
Employee_First_Name,
Employee_Email,
Week_starting,
All_hours,
Submitted_hours,
Rejected_hours,
Approved_hours,
Record_captured_at
FROM Flatten_2
order by ID
),

Remove_null as 
(
    SELECT * from Fill_Nulls
    where Employee_First_Name is not null and Employee_First_Name !=''
),

Form_Record_id as
(
    SELECT {{ dbt_utils.surrogate_key('Employee','Week_starting') }} as Record_id,* 
    from Remove_null
),

DataType_Change as 
(
    SELECT
        Record_id,
        Department::varchar(100) as Department,
        Client::varchar(100) as Client,
        Project::varchar(100) as Project,
        Employee::varchar(100) as Employee,
        Employee_First_Name::varchar(100) as Employee_first_name,
        Employee_Email::varchar(100) as Employee_email,
        Week_starting::date as Week_starting,
        All_hours::number(38,2) as All_hours,
        Submitted_hours::number(38,2) as Submitted_hours,
        Rejected_hours::number(38,2) as Rejected_hours,
        Approved_hours::number(38,2) as Approved_hours,
        Record_captured_at
        FROM Form_Record_id
        ORDER BY ID
)

select * from DataType_Change