{{
    config (
        alias = 'tb_retention_rate',
        transient = false
    )
}}

with retention_value as( 
    select 
        ( year(date_of_joining) || ' '||  monthname(date_of_joining)) as year_month,
        count(employee_id) as retention_rate
    from 
        {{  ref('TB_EMPLOYEE_FORM') }}
    where 
        year(date_of_joining) <> year(date_of_exit)
        and
        month(date_of_joining) <> month(date_of_exit)
        and 
        (date_of_joining) <> '0001-01-01'
    group by year_month
    order by year_month
),

attrition_value as(
        select  
        ( year(date_of_exit) || ' '||  monthname(date_of_exit)) as year_month,
        count(employee_id) as attrition_rate
    from 
        -- "ZOHO_OA_EDW"."ANALYTICS"."TB_EMPLOYEE" 
        {{  ref('TB_EMPLOYEE_FORM') }}
    where 
        year(date_of_joining) <> year(date_of_exit)
        and
        month(date_of_joining) <> month(date_of_exit)
        and 
        date_of_exit <> '0001-01-01'
        and
        date_of_exit is not NULL
    group by year_month
    order by year_month
),

retention_attrition as(
    select 
        ifnull(retention_value.year_month, attrition_value.year_month) as year_month,
        retention_rate,
        attrition_rate
    from 
        retention_value
    full outer join
        attrition_value
        on
        retention_value.year_month = attrition_value.year_month
    order by year_month
)

select * from retention_attrition
