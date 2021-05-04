{{
    config (
        alias = 'user_details'
    )
}}

with user_details_src as (

    select * from {{ source('open_air', 'users_openair') }}

),

casting_users as (

    select 
        vm.value:first::varchar as first_name, 
        vm.value:middle::varchar as middle_name,
        vm.value:last::varchar as last_name,
        vm.value:email::varchar as email_id,
        usr.value:id::number as user_id
    from user_details_src
        ,lateral flatten(input => userdata:User) usr
        ,lateral flatten(input => usr.value:addr) vm

)

select * from casting_users