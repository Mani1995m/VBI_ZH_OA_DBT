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
        count(employee_id) as new_employee_count
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

exit_employee_count as(
     select  
        ( year(date_of_exit) || ' '||  monthname(date_of_exit)) as year_month,
        (year(date_of_exit)*100) + month(date_of_exit) as yr_mon_int,
        count(employee_id) as exit_employee_count
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

employee_join_exit_count as(
    select 
        ifnull(new_employee_count.year_month, exit_employee_count.year_month) as year_month,
        ifnull(new_employee_count.yr_mon_int, exit_employee_count.yr_mon_int) as yr_mon_int,
        new_employee_count,
        exit_employee_count
    from 
        new_employee_count
    full outer join
        exit_employee_count
        on
        new_employee_count.year_month = exit_employee_count.year_month
    order by yr_mon_int
)

select * from employee_join_exit_count
