{{
    config (
        alias = 'tb_reporting_changes',
        transient = false
    )
}}


with reporting_changes as(
    select * from {{ref('TB_REPORTING_CHANGES_FORM')}}
)

select * from reporting_changes