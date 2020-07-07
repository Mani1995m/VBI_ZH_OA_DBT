{{
    config (
        alias = 'timesheet_details'
    )
}}

with timesheet_details_src as (

    select * from {{ source('open_air', 'timesheet_openair') }}

),

casting_timesheet as (

    select 
        ts.value:id::number as timesheet_id, 
        ts.value:userid::number as timesheet_by_user, 
        ts.value:total::number as total_hours, 
        ts.value:status::varchar as status,
        try_to_date(concat(crtdate.value:year,'-',crtdate.value:month,'-',crtdate.value:day)) as created_date,
        try_to_date(concat(strtdate.value:year,'-',strtdate.value:month,'-',strtdate.value:day)) as start_date,
        try_to_date(concat(findate.value:year,'-',findate.value:month,'-',findate.value:day)) as end_date,
        try_to_date(concat(subdate.value:year,'-',subdate.value:month,'-',subdate.value:day)) as submitted_date,
        try_to_date(concat(updtdate.value:year,'-',updtdate.value:month,'-',updtdate.value:day)) as updated_date
    from timesheet_details_src
        , lateral flatten(input => timesheet:Timesheet) ts
        , lateral flatten(input => ts.value:created) crtdate
        , lateral flatten(input => ts.value:starts) strtdate
        , lateral flatten(input => ts.value:ends) findate
        , lateral flatten(input => ts.value:submitted) subdate
        , lateral flatten(input => ts.value:updated) updtdate

)

select * from casting_timesheet