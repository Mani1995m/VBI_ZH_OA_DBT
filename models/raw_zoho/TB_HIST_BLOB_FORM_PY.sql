{% set form_list = ['employee',
                    'Project_Department_Changes',
                    'designation',
                    'Designation_Change_Request',
                    'Intercompany_Transfer',                    
                    'Reporting_Changes'
] %}

{% set delta_query %}
    use schema RAW_ZOHO;
    alter session set ERROR_ON_NONDETERMINISTIC_MERGE = false;
    {% for form in form_list %}
        merge into "ZOHO_OA_EDW"."RAW_ZOHO"."TB_HIST_{{form|upper}}_PY" as target using
            (  select 
                convert_timezone('UTC', current_timestamp(2))::timestamp_ntz as record_captured_at,
                key as record_id, 
                value[0] as SRC1 
               from
                    @STAGE_ZH_HIST_BLOB/{{form}}.json (file_format => FF_JSON_ZOHO) as S, lateral flatten( input => $1 )
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