with

source as (

    select * from {{ source('local_bike', 'stocks') }}

),

renamed as (

    select
        -- ids
        concat(store_id, '_', product_id) as stock_id,
        store_id,
        product_id,

       -- numerics
        quantity as stock_quantity

    from source

)

select * from renamed