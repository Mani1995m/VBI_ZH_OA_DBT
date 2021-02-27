{{
    config (
        alias = 'tb_employee',
        transient = false
    )
}}


with employee_master as(
    select * from {{ref('TB_EMPLOYEE_FORM')}}
)

select * from employee_master