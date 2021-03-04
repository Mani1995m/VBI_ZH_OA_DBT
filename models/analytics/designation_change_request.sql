{{
    config (
        alias = 'tb_designation_change_request',
        transient = false
    )
}}


with designation_change_request as(
    select * from {{ref('TB_DESIGNATION_CHANGE_FORM')}}
)

select * from designation_change_request