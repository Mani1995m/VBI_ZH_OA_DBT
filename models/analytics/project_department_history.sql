{{
    config (
        alias = 'tb_project_department_history',
        transient = false
    )
}}

with emp as(
    select * from {{  ref('TB_EMPLOYEE_FORM') }}
),

proj as(
    select * from {{ref('TB_PROJECT_DEPARTMENT_CHANGES_FORM')}}
)

select 
    employee, 
    emp.first_name as first_name,
    emp.last_name as last_name,
    current_project_department as previous_project_department,
    new_project_department as current_project_department,
    current_proj_department as previous_project,    
    new_proj_department as current_project,
    iff(effective_date='0001-01-01',emp_join_date.date_of_joining, effective_date)   as start_date,
    
    COALESCE(DATEADD(day, -1, lead(effective_date) OVER (PARTITION BY employee ORDER BY start_date)),'9999-12-31') as end_date 

from 
    proj 
    left outer join         
        emp
        on
            proj.employee= emp.employee_id
        
    left outer join
        emp as emp_join_date
        on
          proj.employee = emp_join_date.employee_id
                    
    order by employee, start_date 