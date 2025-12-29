with

source as (

    select * from {{ source('local_bike', 'stocks') }}

),

renamed as (

    select
        -- ids
        store_id,
        product_id,

       -- numerics
        quantity as stock_quantity

    from source

)

select * from renamed