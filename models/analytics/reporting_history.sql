{{
    config (
        alias = 'tb_employee_reporting_history',
        transient = false
    )
}}


with reporting_changes as(
    select * from {{ref('TB_REPORTING_CHANGES_FORM')}}
),
emp as(
    select * from {{  ref('TB_EMPLOYEE_FORM') }}
)

select * from (
    select 
        a.employee_id as employee_zoho_id,
        a.employee as employee_id,
        b.first_name ||' '|| b.last_name as employee_name,
        a.approval_status as people_manager_change_approval_status,
        effective as valid_from, 
        coalesce(dateadd(day,-1,lead(effective) 
        over (partition by a.employee_id order by effective)),current_date() ) as valid_to,
        will_be_reporting_to as people_manager_id,
        c.first_name ||' '|| c.last_name as people_manager_name,

        datediff(day, valid_from, valid_to) as no_of_days,
        datediff(month, valid_from, valid_to) as no_of_months,
        to_number(no_of_months/12,5, 1) as no_of_years
        from 
            reporting_changes a
            left outer join
            emp b
            on a.employee_id = b.zoho_id

            left outer join 
            emp c
            on a.will_be_reporting_to_id =c.zoho_id

    union all

    select 
        employee_zoho_id,
        a.employee_id,
        b.first_name ||' '|| b.last_name as employee_name,
        'Approved' as people_manager_change_approval_status,

        case 
            when b.original_doj_for_transfers is not null
                then b.original_doj_for_transfers										
            when b.date_of_start_of_internship is not null
                then b.date_of_start_of_internship
            when b.date_of_joining is not null
                then b.date_of_joining
            else 
                NULL
        end as valid_from,
        case
            when effective is not null 
                then effective
            else 
                NULL
        end as valid_to,

        people_manager_id,
        c.first_name ||' '|| c.last_name as people_manager_name,
        datediff(day, valid_from, valid_to) as no_of_days,
        datediff(month, valid_from, valid_to) as no_of_months,
        to_number(no_of_months/12,5, 1) as no_of_years
        
        from
            (select 
                employee_id as employee_zoho_id,
                employee as employee_id,
                effective, 
                currently_reporting_to as people_manager_id,
                currently_reporting_to_id as people_manager_zoho_id,
                rank()over (partition by employee_id order by effective) as rec_no
                from reporting_changes )a
                    left outer join
                    emp b
                    on employee_zoho_id = b.zoho_id
                    left outer join 
                    emp c
                    on people_manager_zoho_id = c.zoho_id
                where rec_no =  1)
order by employee_id, valid_from

