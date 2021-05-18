{{
    config (
        alias = 'tb_employee_exit',
        transient = false
    )
}}


with employee_master as(
    select * from {{ref('TB_EMPLOYEE_FORM')}}
)

select 
    ( year(date_of_exit) || '-' || month(date_of_exit) || '-01' ) as yr_mon_int,
    date_of_exit,
    employee_id,
    email_id,
    first_name,
    last_name,
    reporting_to,
    reportingto_emailid
from 
    employee_master