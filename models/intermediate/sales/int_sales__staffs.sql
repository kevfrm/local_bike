with staffs as (

    select * from {{ ref('stg_sales__staffs') }}

)

select 
    staff_id,
    store_id,
    manager_id,
    concat(staff_first_name, ' ', staff_last_name) as staff_name,
    staff_email,
    staff_phone,
    staff_active
from staffs