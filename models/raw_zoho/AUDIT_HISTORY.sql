{{
    config(
        materialzed = 'ephemeral'
    )
}}

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

{% set audit_table %}
    {% for view in view_list %}
        merge into "ZOHO_OA_EDW"."ANALYTICS"."TB_AUDIT_HIST" as target using
            (select max('TB_HIST_{{view|upper}}') as historytable,  max("RECORD_CAPTURED_AT") as lastupdated from "ZOHO_OA_EDW"."RAW_ZOHO"."TB_HIST_{{view|upper}}") as source
            on
            target.historytable = source.historytable
        when matched then
            update set target.historytable = source.historytable, target.lastupdated = source.lastupdated
        when not matched then
            insert (historytable, lastupdated) values(source.historytable, source.lastupdated);
    {% endfor %}
{% endset %}

{% do run_query(audit_table) %}

select convert_timezone('UTC', current_timestamp(2))::timestamp_ntz as last_delta_load