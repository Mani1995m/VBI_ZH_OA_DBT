{{
    config (
        alias = 'project_details'
    )
}}

with project_details_src as (

    select * from {{ source('open_air', 'project_openair') }}

),

casting_project as (

    select 
        prj.value:id::number as project_id,
        prj.value:name::varchar as project_name,
        prj.value:active::number as project_active,
        prj.value:customer_name::varchar as customer_name, 
        prj.value:customerid::number as customer_id,
        prj.value:picklist_label::varchar as project_picklistlabel,
        prj.value:userid::number as project_owner_id,
        try_to_date(concat(crtdate.value:year,'-',crtdate.value:month,'-',crtdate.value:day)) as created_date,
        try_to_date(concat(strtdate.value:year,'-',strtdate.value:month,'-',strtdate.value:day)) as start_date,
        try_to_date(concat(findate.value:year,'-',findate.value:month,'-',findate.value:day)) as finish_date,
        try_to_date(concat(updtdate.value:year,'-',updtdate.value:month,'-',updtdate.value:day)) as updated_date
    from project_details_src
        , lateral flatten(input => project:Project) prj
        , lateral flatten(input => prj.value:created) crtdate
        , lateral flatten(input => prj.value:finish_date) findate
        , lateral flatten(input => prj.value:start_date) strtdate
        , lateral flatten(input => prj.value:updated) updtdate

)

select * from casting_project