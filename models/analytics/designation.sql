{{
    config (
        alias = 'tb_designation',
        transient = false
    )
}}


with designation as(
    select * from {{ref('TB_DESIGNATION_FORM')}}
)

select * from designation