{{
    config (
        alias = 'tb_retention_rate',
        transient = false
    )
}}


with retention_rate as( 
    select 
        ( year(date_of_joining) || ' '||  monthname(date_of_joining)) as year_month,
        count(employee_id) as joining_trend
    from 
        {{  ref('TB_EMPLOYEE_FORM') }}
    where 
        year(date_of_joining) <> year(date_of_exit)
        and
        month(date_of_joining) <> month(date_of_exit)
    group by year_month

)

select * from retention_rate