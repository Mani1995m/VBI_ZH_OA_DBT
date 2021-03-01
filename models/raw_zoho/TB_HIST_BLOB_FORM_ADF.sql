{% set form_list = ['employee_adf',
                    'Project_Department_Changes_adf',
                    'designation_adf',
                    'Designation_Change_Request_adf',
                    'Intercompany_Transfer_adf',                    
                    'Reporting_Changes_adf'
] %}

{% set delta_query %}
    use schema RAW_ZOHO;
    alter session set ERROR_ON_NONDETERMINISTIC_MERGE = false;
    {% for form in form_list %}
        merge into "ZOHO_OA_EDW"."RAW_ZOHO"."TB_HIST_{{form|upper}}" as target using
            (  select 
                convert_timezone('UTC', current_timestamp(2))::timestamp_ntz as record_captured_at,
                l4.key as record_id, 
                l4.value[0] as SRC1 
               from
                    @STAGE_ZH_HIST_BLOB/vbidw/zoho/{{form}}/{{form}}.json (file_format => FF_JSON_ZOHO) as S, 
                    lateral flatten( input => $1 ) as l1,
                    lateral flatten(input => l1.value )l2,
                    lateral flatten(input => l2.value )l3,
                    lateral flatten(input => l3.value )l4
                    
            ) as sources on
            sources.record_id = target.record_id 
        when matched then 
            update set target.RECORD_CAPTURED_AT = sources.RECORD_CAPTURED_AT, target.record_id = sources.record_id, target.SRC = sources.SRC1
        when not matched then
            insert (record_captured_at, record_id, SRC) values(sources.record_captured_at, sources.record_id, sources.SRC1);

    {% endfor %}
{% endset %}

{% do run_query(delta_query) %}

select convert_timezone('UTC', current_timestamp(2))::timestamp_ntz as last_delta_load