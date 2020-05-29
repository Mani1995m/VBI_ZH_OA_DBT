



select count(*)
from (

    select
        id

    from ZOHO_OA_EDW.ANALYTICS.my_first_dbt_model
    where id is not null
    group by id
    having count(*) > 1

) validation_errors

