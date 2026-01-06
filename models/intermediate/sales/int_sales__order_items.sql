with order_items as (

    select * from {{ ref('stg_sales__order_items') }}

),

orders as (

    select * from {{ ref('int_sales__orders') }}

),

get_info_at_order_item_level as (

    select 
        -- order item info
        order_item.order_item_id as order_item_id,
        order_item.order_id as order_id,
        order_item.order_item_discount as order_item_discount,
        order_item.order_item_quantity as order_item_quantity,
        order_item.total_order_item_amount as total_order_item_amount,
        order_item.product_id as product_id,
        -- customer info
        s_order.customer_id as customer_id,
        -- store info
        s_order.store_id as store_id,
        -- staff info
        s_order.staff_id as staff_id,
        -- order info
        s_order.order_date as order_date        
    from
        order_items order_item left join orders s_order on order_item.order_id = s_order.order_id
    
)

select * from get_info_at_order_item_level