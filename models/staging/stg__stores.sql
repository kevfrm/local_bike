with

source as (

    select * from {{ source('local_bike', 'stores') }}

),

renamed as (

    select
        -- ids
        store_id,

       -- strings
        store_name,
        phone as store_phone,
        email as store_email,
        street as store_street,
        city as store_city,
        state as store_state,

        -- numerics
        zip_code as store_zip_code        

    from source

)

select * from renamed