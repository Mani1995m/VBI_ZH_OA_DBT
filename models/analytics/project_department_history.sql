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

Select * from (
    select 
        employee, 
        emp.first_name as first_name,
        emp.last_name as last_name,
        current_project_department as previous_project_department,
        new_project_department as current_project_department,
        current_proj_department as previous_project,    
        new_proj_department as current_project,
        effective_date as start_date,
        -- To fetch next row effective date subtract by -1 to set end date of current project record
        COALESCE( -- return first non-null expression
            DATEADD(
                day,
                -1,
                lead(effective_date) OVER(PARTITION BY employee ORDER BY effective_date)
            ),
            current_date() 
        ) as end_date, 
        datediff(day, start_date, end_date) as no_of_days,
        datediff(month, start_date, end_date) as no_of_months,
        to_number(no_of_months/12,5, 1) as no_of_years

    from 
        proj 
        left outer join         
            emp
            on
                proj.employee= emp.employee_id
  
    union all	
    -- To add start date as Date of Joining for the first record of each employee. Because all employee starting 
        select 														
                                                
            a.employee,
            b.first_name as first_name,
            b.last_name as last_name,
            NULL AS previous_project_department,
            current_project_department ,
            NULL AS previous_proj_department,
            current_proj_department,	
            case 
                when b.original_doj_for_transfers is not null
                    then b.original_doj_for_transfers										
                when b.date_of_start_of_internship is not null
                    then b.date_of_start_of_internship
                when b.date_of_joining is not null
                    then b.date_of_joining
                else 
                    NULL
            end as start_date,			
            case
                when effective_date is not null 
                    then effective_date
                else 
                    NULL
            end as end_date,
            datediff(day, start_date, end_date) as no_of_days,
            datediff(month, start_date, end_date) as no_of_months,
            to_number(no_of_months/12,5, 1) as no_of_years
                                    
            from														
                (select																									
                    employee,												
                    current_project_department ,
                    current_proj_department,    												
                    effective_date,													
                    rank()over (partition by employee_id order by effective_date) as rec_no						
                    from proj)	a								
                        left outer join													
                        emp b												
                        on a.employee= b.employee_id											
                    where rec_no =  1
    )
    where 
        length(employee) !=0 
    order by employee, start_date 