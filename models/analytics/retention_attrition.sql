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

select * from retention_attrition
