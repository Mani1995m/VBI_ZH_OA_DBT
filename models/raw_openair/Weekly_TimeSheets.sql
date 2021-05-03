{{
    config (
        alias ='Weekly_TimeSheets',
        materialized = 'incremental'
        )
}}

{%- call statement('my_statement', fetch_result=True) -%}
      SELECT top 1 case when $2 like '%/%' then right(trim(SPLIT($2,'-')[0]),10) else NULL end
       FROM @STAGE_OA_BLOB/vbidw/Openair/Weekly_Validation_1_report.csv (file_format => FF_CSV_OA)
{%- endcall -%}


{%- set my_var = load_result('my_statement')['data'][0][0] -%}

with Flatten_1 as 
(
    SELECT $1 as C1,
    case when len($1)-len(ltrim($1)) = 0 then 'Department'
     when len($1)-len(ltrim($1)) = 5 then 'Client'
     when len($1)-len(ltrim($1)) = 10 then 'Project'
     else 'Employee'
     end as Hierarchy_level,
     $2 as All_hours,
     $3 as Submitted_hours,
     $4 as Rejected_hours,
     $5 as Approved_hours
     FROM @STAGE_OA_BLOB/vbidw/Openair/Weekly_Validation_1_report.csv (file_format => FF_CSV_OA)
     where $1 not like '%/%' and $1 !='' and $1 is not null
),

Flatten_2 as (
       SELECT  C1,Hierarchy_level,
       CASE WHEN Hierarchy_level = 'Department' then trim(C1) end as Department,
       CASE WHEN Hierarchy_level = 'Client' then trim(C1) end as Client,
       CASE WHEN Hierarchy_level = 'Project' then trim(C1) end as Project,
       CASE WHEN Hierarchy_level = 'Employee' then trim(C1) end as Employee,
       All_hours,
       Submitted_hours,
       Rejected_hours,
       Approved_hours
       from Flatten_1
),

generate_unqiue_rownum as (
    SELECT ROW_NUMBER() OVER(ORDER BY NULL) as ID,
    C1,
    Hierarchy_level,
    Department,
    Client,
    Project,
    Employee,
    All_hours,
    Submitted_hours,
    Rejected_hours,
    Approved_hours
    from Flatten_2
),

Fill_Nulls as
(
SELECT
case when Department is null then LAG(Department) ignore nulls OVER(ORDER BY ID) else Department end as Department,
case when Client is null then LAG(Client) ignore nulls OVER(ORDER BY ID) else Client end as Client,
case when Project is null then LAG(Project) ignore nulls OVER(ORDER BY ID) else Project end as Project,
Employee,
'{{ my_var }}'as Week_Starting,
All_hours,
Submitted_hours,
Rejected_hours,
Approved_hours,
convert_timezone('Asia/Calcutta', current_timestamp(2))::timestamp_ntz as Record_captured_at
FROM generate_unqiue_rownum
order by ID
),

Remove_null as 
(
    SELECT * from Fill_Nulls
    where Employee is not null and Employee !=''
),

Final as 
(
    SELECT * from Remove_null
)

SELECT * FROM Final