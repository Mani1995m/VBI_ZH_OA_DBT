{{ 
    config(materialized='table')
}}



with DEPARTMENT AS (
    select 
ID :: INT AS ID,
NAME :: NVARCHAR(100) AS NAME,
NOTES :: NVARCHAR(250) AS NOTES,
DELETED :: INT AS DELETED,
CREATED :: TIMESTAMP AS CREATED

from  {{ source('OPEN_AIR_FORM', 'TB_HIST_DEPARTMENT')}} 
"ZOHO_OA_EDW"."RAW_OPENAIR"."DEPARTMENT"
)
select * from DEPARTMENT
