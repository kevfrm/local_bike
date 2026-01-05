with int_order_items as (

    select * from {{ ref('int_order_items') }}

),

aggregate_product_orders_info as (
    
    select
        int_order_item.order_date as reporting_date,
        sum(int_order_item.order_item_quantity) as product_order_quantity,
        sum(int_order_item.total_order_item_amount) as total_product_order_amount,
        max(int_order_item.store_name) as store_name,
        max(int_order_item.store_street) as store_street,
        max(int_order_item.store_city) as store_city,
        max(int_order_item.store_zip_code) as store_zip_code,
        max(int_order_item.product_name) as product_name,
        max(int_order_item.brand_name) as brand_name,
        max(int_order_item.category_name) as category_name
    from 
        int_order_items int_order_item
    group by
        store_id,
        product_id,
        order_date
)

select * from aggregate_product_orders_info