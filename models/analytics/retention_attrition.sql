{{
    config (
        alias = 'tb_retention_attrition',
        transient = false
    )
}}


with retention_value as( 
    select 
        ( year(date_of_joining) || ' '||  monthname(date_of_joining)) as year_month,
        (year(date_of_joining)*100) + month(date_of_joining) as yr_mon_int,
        count(employee_id) as retention_rate
    from 
        {{  ref('TB_EMPLOYEE_FORM') }}
    where 
        year(date_of_joining) <> year(date_of_exit)
        and
        month(date_of_joining) <> month(date_of_exit)
        and 
        (date_of_joining) <> '0001-01-01'
    group by year_month,yr_mon_int
),

attrition_value as(
     select  
        ( year(date_of_exit) || ' '||  monthname(date_of_exit)) as year_month,
        (year(date_of_exit)*100) + month(date_of_exit) as yr_mon_int,
        count(employee_id) as attrition_rate
    from 
        {{  ref('TB_EMPLOYEE_FORM') }}
    where 
        year(date_of_joining) <> year(date_of_exit)
        and
        month(date_of_joining) <> month(date_of_exit)
        and 
        date_of_exit <> '0001-01-01'
        and
        date_of_exit is not NULL
    group by year_month, yr_mon_int
),

retention_attrition as(
    select 
        ifnull(retention_value.year_month, attrition_value.year_month) as year_month,
        ifnull(retention_value.yr_mon_int, attrition_value.yr_mon_int) as yr_mon_int,
        retention_rate,
        attrition_rate
    from 
        retention_value
    full outer join
        attrition_value
        on
        retention_value.year_month = attrition_value.year_month
    order by yr_mon_int
)

month_start_count as
(

select  ( year(MONTH_START_DATE) || ' '||  monthname(MONTH_START_DATE)) as year_month,
        (year(MONTH_START_DATE)*100) + month(MONTH_START_DATE) as yr_mon_int,
count(employee_id) Active_Emp_Month_Begin
from (select distinct MONTH_START_DATE from "ZOHO_OA_EDW"."ANALYTICS"."DATE_DIM") X
left join "ZOHO_OA_EDW"."ANALYTICS"."TB_EMPLOYEE"
on DATE_OF_JOINING<MONTH_START_DATE
and (DATE_OF_EXIT>MONTH_START_DATE OR (DATE_OF_EXIT='0001-01-01'))
where DATE_OF_JOINING!='0001-01-01'
group by MONTH_START_DATE
)

month_end_count as(

select left(MONTH_END_DATE,7)Month,count(employee_id) Active_Emp_Month_End  
from (select distinct MONTH_END_DATE from "ZOHO_OA_EDW"."ANALYTICS"."DATE_DIM") X
left join "ZOHO_OA_EDW"."ANALYTICS"."TB_EMPLOYEE"
on DATE_OF_JOINING<MONTH_END_DATE
and (DATE_OF_EXIT>MONTH_END_DATE OR (DATE_OF_EXIT='0001-01-01'))
where DATE_OF_JOINING!='0001-01-01'
group by MONTH_END_DATE
)

month_start_end as(
select A.year_month,A.Active_Emp_Month_Begin,B.Active_Emp_Month_End from
month_start_count A
left join month_end_count B
on A.year_month=B.year_month
)

final as 
(

)
select * from retention_attrition
