{{
    config (
        alias = 'tb_intercompany_transfer',
        transient = false
    )
}}


with intercompany_transfer as(
    select * from {{ref('TB_INTERCOMPANY_TRANSFER_FORM')}}
),
emp as(
    select * from {{  ref('TB_EMPLOYEE_FORM') }}
)

select 
    a.employee_id as employee_zoho_id,
    a.employee as employee_id,
    b.first_name ||' '|| b.last_name as employee_name,
    a.approval_status as transfer_approval_status,
    a.approval_time as transfer_approval_time,
    emergency_contact_address,
    emergency_contact_person,
    emergency_contact_number
    from 
        intercompany_transfer a
        left outer join
        emp b
        on a.employee_id = b.zoho_id
order by employee_id,transfer_approval_time
