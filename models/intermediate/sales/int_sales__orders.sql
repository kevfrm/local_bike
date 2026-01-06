with orders as (

    select * from {{ ref('stg_sales__orders') }}

),

order_items as (

    select * from {{ ref('stg_sales__order_items') }}

),

get_info_at_order_level as (

    select 
        -- order info
        s_order.order_id as order_id,
        max(s_order.order_status) as order_status,
        max(s_order.order_date) as order_date,
        round(sum(order_item.total_order_item_amount), 2) as total_order_amount,
        -- customer info
        max(s_order.customer_id) as customer_id,
        -- store info
        max(s_order.store_id) as store_id,
        -- staff info
        max(s_order.staff_id) as staff_id
    from
        orders s_order left join order_items order_item on s_order.order_id = order_item.order_id
    group by
      order_id
      
)

select * from get_info_at_order_level