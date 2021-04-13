{{
    config (
        alias = 'tb_employee_join_exit_count',
        transient = false
    )
}}


with new_employee_count as( 
    select 
        ( year(date_of_joining) || ' '||  monthname(date_of_joining)) as year_month,
        (year(date_of_joining)*100) + month(date_of_joining) as yr_mon_int,
        department,
        employee_type,
        count(employee_id) as new_employee_count
    from 
        {{  ref('TB_EMPLOYEE_FORM') }}
    where 
        (date_of_joining) <> '0001-01-01'
    group by year_month,yr_mon_int,department,employee_type
),

exit_employee_count as(
     select  
        ( year(date_of_exit) || ' '||  monthname(date_of_exit)) as year_month,
        (year(date_of_exit)*100) + month(date_of_exit) as yr_mon_int,
        department,
        employee_type,
        count(employee_id) as exit_employee_count
    from 
        {{  ref('TB_EMPLOYEE_FORM') }}
    where 
        date_of_exit <> '0001-01-01'
        and
        date_of_exit is not NULL
        and
        date_of_joining <> '0001-01-01'
    group by year_month, yr_mon_int,department,employee_type
),

employee_join_exit_count as(
    select 
        ifnull(new_employee_count.year_month, exit_employee_count.year_month) as year_month,
        ifnull(new_employee_count.yr_mon_int, exit_employee_count.yr_mon_int) as yr_mon_int,
        ifnull(new_employee_count.department, exit_employee_count.department) as department,
        ifnull(new_employee_count.employee_type, exit_employee_count.employee_type) as employee_type,
        new_employee_count,
        exit_employee_count
    from 
        new_employee_count
    full outer join
        exit_employee_count
        on
        new_employee_count.year_month = exit_employee_count.year_month
        and new_employee_count.department = exit_employee_count.department
        and new_employee_count.employee_type = exit_employee_count.employee_type
    order by yr_mon_int
),

month_start_count as
(

select  ( year(MONTH_START_DATE) || ' '||  monthname(MONTH_START_DATE)) as year_month,
        (year(MONTH_START_DATE)*100) + month(MONTH_START_DATE) as yr_mon_int,
        department,
        employee_type,
count(employee_id) Active_Emp_Month_Begin
from (select distinct MONTH_START_DATE from "ZOHO_OA_EDW"."ANALYTICS"."DATE_DIM") X
left join "ZOHO_OA_EDW"."ANALYTICS"."TB_EMPLOYEE"
on DATE_OF_JOINING<MONTH_START_DATE
and (DATE_OF_EXIT>MONTH_START_DATE OR (DATE_OF_EXIT='0001-01-01'))
where DATE_OF_JOINING!='0001-01-01'
group by MONTH_START_DATE,department,employee_type
),

month_end_count as(

select ( year(MONTH_END_DATE) || ' '||  monthname(MONTH_END_DATE)) as year_month,
        (year(MONTH_END_DATE)*100) + month(MONTH_END_DATE) as yr_mon_int,
        department,
        employee_type,
        count(employee_id) Active_Emp_Month_End  
from (select distinct MONTH_END_DATE from "ZOHO_OA_EDW"."ANALYTICS"."DATE_DIM") X
left join "ZOHO_OA_EDW"."ANALYTICS"."TB_EMPLOYEE"
on DATE_OF_JOINING<MONTH_END_DATE
and (DATE_OF_EXIT>MONTH_END_DATE OR (DATE_OF_EXIT='0001-01-01'))
where DATE_OF_JOINING!='0001-01-01'
group by MONTH_END_DATE,department,employee_type
),

month_start_end as(
select A.year_month,A.yr_mon_int,
A.department,A.employee_type,
A.Active_Emp_Month_Begin,B.Active_Emp_Month_End from
month_start_count A
left join month_end_count B
on A.year_month=B.year_month
and A.department = B.department
and A.employee_type = B.employee_type
),

final as 
(
select  A.year_month,
        A.yr_mon_int,
        A.department,
        A.employee_type,
        new_employee_count,
        exit_employee_count,
        Active_Emp_Month_Begin,
        Active_Emp_Month_End
from employee_join_exit_count A 
left join month_start_end B
on A.year_month=B.year_month
and A.department = B.department
and A.employee_type = B.employee_type
)
select * from final
order by yr_mon_int