with

source as (

    select * from {{ source('local_bike', 'customers') }}

),

renamed as (

    select
        -- ids
        customer_id,
        
        -- strings
        first_name as customer_first_name,
        last_name as customer_last_name,
        case when phone = 'NULL' then null else phone end as customer_phone,
        email as customer_email,
        street as customer_street,
        city as customer_city,
        state as customer_state,

        -- numerics
        zip_code as customer_zip_code

    from source

)

select * from renamed