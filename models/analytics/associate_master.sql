{{
    config (
        alias = 'vbi_master_data',
        transient = false
    )
}}

with zoho_master as (

    select * from {{ ref('TB_ASSOCIATE_MASTER') }}

),

openair_master as (

    select * from {{ ref('Users') }}

),

join_zoho_oa as (

    select 
        zoho.*,
        oa.user_id as Openair_id  
    from zoho_master zoho
    inner join openair_master oa
        on zoho.email_id = oa.email_id 

)

select * from join_zoho_oa