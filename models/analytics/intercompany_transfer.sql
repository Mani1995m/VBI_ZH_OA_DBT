{{
    config (
        alias = 'tb_intercompany_transfer',
        transient = false
    )
}}


with intercompany_transfer as(
    select * from {{ref('TB_INTERCOMPANY_TRANSFER_FORM')}}
)

select * from intercompany_transfer