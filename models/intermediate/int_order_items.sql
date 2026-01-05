with order_items as (

    select * from {{ ref('stg_sales__order_items') }}

),

orders as (

    select * from {{ ref('int_orders') }}

),

products as (

    select * from {{ ref('stg_sales__products') }}

),

brands as (

    select * from {{ ref('stg_sales__brands') }}

),

categories as (

    select * from {{ ref('stg_sales__categories') }}

),

get_detailed_info_at_order_item_level as (

    select 
        order_item.order_item_id as order_item_id,
        order_item.order_id as order_id,
        order_item.order_item_discount as order_item_discount,
        order_item.order_item_quantity as order_item_quantity,
        order_item.total_order_item_amount as total_order_item_amount,
        -- order info
        s_order.customer_name as customer_name,
        s_order.customer_city as customer_city,
        s_order.customer_state as customer_state,
        s_order.customer_zip_code as customer_zip_code,
        s_order.store_id as store_id,
        s_order.store_name as store_name,
        s_order.store_street as store_street,
        s_order.store_city as store_city,
        s_order.store_zip_code as store_zip_code,
        s_order.staff_id as staff_id,
        s_order.staff_name as staff_name,
        s_order.order_date as order_date,
        -- product info
        order_item.product_id as product_id,
        product.product_name as product_name,
        product.product_price as product_price,
        product.brand_id as brand_id,
        brand.brand_name as brand_name,
        product.category_id as category_id,
        category.category_name as category_name
    from
        order_items order_item left join orders s_order on order_item.order_id = s_order.order_id
        left join products product on order_item.product_id = product.product_id
        left join brands brand on product.brand_id = brand.brand_id
        left join categories category on product.category_id = category.category_id
    
)

select * from get_detailed_info_at_order_item_level