{{
    config (
        alias = 'tb_project_departmenet_changes',
        transient = false
    )
}}


with project_department_changes as(
    select * from {{ref('TB_PROJECT_DEPARTMENT_CHANGES_FORM')}}
)

select * from project_department_changes