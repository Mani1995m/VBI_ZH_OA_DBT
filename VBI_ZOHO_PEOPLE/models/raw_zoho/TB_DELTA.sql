{% set view_list = ['Associate_View',
                    'Designation_Change_Request_View',
                    'Designation_Changes_approver_view',
                    'EmployeeInactiveView',
                    'Leave_approver_view',
                    'Month_View',
                    'P_ApplyLeaveView',
                    'P_DepartmentView',
                    'P_DesignationView',
                    'P_EmployeeView',
                    'Reporting_Changes_View',
                    'People_Manager_Changes_approver_view',
                    'Project_Department_Changes_View',        
                    'Project_Department_Changes1_approver_view',     
                    'P_AttendanceForm_View'] %}

{% set delta_query %}
    use schema RAW_ZOHO;
    {% for view in view_list %}
        merge into "ZOHO_OA_EDW"."RAW_ZOHO"."TB_HIST_{{view|upper}}" as target using
            (select convert_timezone('UTC', current_timestamp(2))::timestamp_ntz as "RECORD_CAPTURED_AT", $1 as "SRC1" from @STAGE_ZH_DELTA/{{view}}.json.gz (file_format=> FF_JSON_TEST)) as sources on
            parse_json(sources."SRC1"):"recordId" = target.SRC:"recordId" 
        when matched then 
            update set target."RECORD_CAPTURED_AT" = sources."RECORD_CAPTURED_AT", target."SRC" = sources."SRC1"
        when not matched then
            insert ("RECORD_CAPTURED_AT", SRC) values(sources."RECORD_CAPTURED_AT", sources."SRC1");
    {% endfor %}
{% endset %}



{% do run_query(delta_query) %}

select convert_timezone('UTC', current_timestamp(2))::timestamp_ntz as last_delta_load