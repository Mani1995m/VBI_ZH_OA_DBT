{{
    config (
        alias = 'tb_employee_join_exit_count',
        transient = false
    )
}}


with new_employee_count as( 
    select 
        ( year(date_of_joining) || ' '||  monthname(date_of_joining)) as year_month,
        ( year(date_of_joining) || '-' || month(date_of_joining) || '-01' ) as yr_mon_int,
        year(date_of_joining) as year_char,
        monthname(date_of_joining) as month_char,
        department,
        employee_type,
        count(employee_id) as new_employee_count
    from 
        {{  ref('TB_EMPLOYEE_FORM') }}
    where 
        (date_of_joining) is not null
    group by year_month,yr_mon_int,year_char,month_char,department,employee_type
),

exit_employee_count as(
     select  
        ( year(date_of_exit) || ' '||  monthname(date_of_exit)) as year_month,
        ( year(date_of_exit) ||'-' || month(date_of_exit) ||'-01' ) as yr_mon_int,
        year(date_of_exit) as year_char,
        monthname(date_of_exit) as month_char,
        department,
        employee_type,
        count(employee_id) as exit_employee_count
    from 
        {{  ref('TB_EMPLOYEE_FORM') }}
    where 
        date_of_exit is not NULL
        and
        date_of_joining is not null
    group by year_month, yr_mon_int,year_char,month_char,department,employee_type
),

employee_join_exit_count as(
    select 
        ifnull(new_employee_count.year_month, exit_employee_count.year_month) as year_month,
        ifnull(new_employee_count.yr_mon_int, exit_employee_count.yr_mon_int) as yr_mon_int,
        ifnull(new_employee_count.year_char, exit_employee_count.year_char) as year_char,
        ifnull(new_employee_count.month_char, exit_employee_count.month_char) as month_char,
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

month_start_count as(
    select  
        year(MONTH_START_DATE) || ' '||  monthname(MONTH_START_DATE) as year_month,
        year(MONTH_START_DATE) ||'-' || month(MONTH_START_DATE) ||'-01'  as yr_mon_int,
        year(MONTH_START_DATE) AS year_char,
        monthname(MONTH_START_DATE) as month_char,
        department, 
        employee_type,
        count(employee_id) Active_Emp_Month_Begin
    from 
        (select distinct MONTH_START_DATE from {{  ref('Date_Dim') }}) X
    left join 
        {{  ref('TB_EMPLOYEE_FORM') }}
        on 
        DATE_OF_JOINING<=MONTH_START_DATE
        and 
        (DATE_OF_EXIT>MONTH_START_DATE OR (DATE_OF_EXIT is null))
    where 
        DATE_OF_JOINING is not null
    group by 
        MONTH_START_DATE,department,employee_type
),

month_end_count as(

    select 
        year(MONTH_END_DATE) || ' '||  monthname(MONTH_END_DATE) as year_month,
        year(MONTH_END_DATE) ||'-' || month(MONTH_END_DATE) ||'-01'  as yr_mon_int,
        year(MONTH_END_DATE) AS year_char,
        monthname(MONTH_END_DATE) as month_char,
        department,
        employee_type,
        count(employee_id) Active_Emp_Month_End  
    from 
        (select distinct MONTH_END_DATE from {{  ref('Date_Dim') }}) X
    left join 
        {{  ref('TB_EMPLOYEE_FORM') }}
        on 
        DATE_OF_JOINING<=MONTH_END_DATE
        and 
        (DATE_OF_EXIT>MONTH_END_DATE OR (DATE_OF_EXIT is null))
    where 
        DATE_OF_JOINING is not null
    group by 
        MONTH_END_DATE,department,employee_type
),

month_start_end as(
    select 
        ifnull(A.year_month, B.year_month) as year_month,
        ifnull(A.yr_mon_int, B.yr_mon_int) as yr_mon_int,
        ifnull(A.year_char, B.year_char) as year_char,
        ifnull(A.month_char, B.month_char) as month_char,
        ifnull(A.department, B.department) as department,
        ifnull(A.employee_type, B.employee_type) as employee_type,
        A.Active_Emp_Month_Begin,
        B.Active_Emp_Month_End 
    from
        month_start_count A
    full outer join 
        month_end_count B
    on
        A.year_month=B.year_month
        and 
        A.department = B.department
        and 
        A.employee_type = B.employee_type
),

final as(
    select  
        B.year_month,
        B.yr_mon_int,
        B.year_char,
        B.month_char,
        B.department,
        B.employee_type,
        new_employee_count,
        exit_employee_count,
        Active_Emp_Month_Begin,
        Active_Emp_Month_End
    from 
        employee_join_exit_count A 
    full outer join 
        month_start_end B
    on
        A.year_month=B.year_month
        and 
        A.department = B.department
        and
        A.employee_type = B.employee_type
)
select 
    * 
from 
    final
order by 
    yr_mon_int