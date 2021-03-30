{{
    config (
        alias = 'tb_designation_change_request',
        transient = false
    )
}}


with designation_change_request as(
    select * from {{ref('TB_DESIGNATION_CHANGE_FORM')}}
),
emp as(
    select * from {{  ref('TB_EMPLOYEE_FORM') }}
)

select * from (													
    select 														
        a.employee_id as employee_zoho_id,										
        a.employee as employee_id,											
        b.first_name ||' '|| b.last_name as employee_name,									
        a.approval_status as designation_change_approval_status,								
        new_designation as designation,											
        effective_from as valid_from,											
        coalesce(dateadd(day,-1,lead(effective_from) 
        over (partition by a.employee_id order by effective_from)),'9999-12-31') as valid_to
        from 
            designation_change_request a										
            left outer join													
            emp b												
            on a.employee_id = b.zoho_id											
														
    union all													
    
    select 														
        employee_zoho_id,												
        a.employee_id,													
        b.first_name ||' '|| b.last_name as employee_name,									
        'Approved' as designation_change_approval_status,									
        current_designation as designation,										
        case when (b.original_doj_for_transfers = '' or b.original_doj_for_transfers is null) 
        and b.date_of_joining = '0001-01-01'	
        then b.date_of_start_of_internship										
        when (b.original_doj_for_transfers = '' or b.original_doj_for_transfers is null) 
        and b.date_of_joining <> '0001-01-01' 		
        then b.date_of_joining else b.original_doj_for_transfers end as valid_from,						
        dateadd(day,-1,effective_from) as valid_to										
        from														
            (select														
                employee_id as employee_zoho_id,											
                employee as employee_id,												
                current_designation,												
                effective_from,													
                rank()over (partition by employee_id order by effective_from) as rec_no						
                from designation_change_request)a										
                    left outer join													
                    emp b												
                    on employee_zoho_id = b.zoho_id 											
                where rec_no =  1)												
														
order by employee_id, valid_from											

