with

source as (

    select * from {{ source('local_bike', 'order_items') }}

),

renamed as (

    select
        -- ids
        concat(order_id, '_', product_id) as order_item_id,
        order_id,
        product_id,

        -- numerics
        item_id as order_item_number,
        quantity as order_item_quantity,
        list_price as order_item_price,
        discount as order_item_discount

    from source

)

select * from renamed