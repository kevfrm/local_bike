with

source as (

    select * from {{ source('local_bike', 'staffs') }}

),

renamed as (

    select
        -- ids
        staff_id,
        store_id,
        cast((case when manager_id = 'NULL' then null else manager_id end) as integer) as manager_id,

        -- strings
        first_name as staff_first_name,
        last_name as staff_last_name,
        email as staff_email,
        phone as staff_phone,

        -- booleans
        active as staff_active

    from source

)

select * from renamed