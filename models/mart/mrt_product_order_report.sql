with int_order_items as (

    select * from {{ ref('int_order_items') }}

),

int_stores as (

    select * from {{ ref('int_stores') }}

),

int_products as (

    select * from {{ ref('int_products') }}

),

aggregate_product_orders_info as (
    
    select
        int_order_item.order_date as reporting_date,
        int_order_item.store_id as store_id,
        int_order_item.product_id as product_id,
        sum(int_order_item.order_item_quantity) as product_order_quantity,
        sum(int_order_item.total_order_item_amount) as total_product_order_amount        
    from 
        int_order_items int_order_item
    group by
        reporting_date,
        store_id,
        product_id
),

measures_and_dimension_info as (
    
    select
        agg_prod_order.reporting_date,
        int_store.store_name as store_name,
        int_store.store_street as store_street,
        int_store.store_city as store_city,
        int_store.store_zip_code as store_zip_code,
        int_product.product_name as product_name,
        int_product.product_brand as product_brand,
        int_product.product_category as product_category,
        agg_prod_order.product_order_quantity,
        agg_prod_order.total_product_order_amount 
    from 
        aggregate_product_orders_info agg_prod_order left join int_stores int_store on agg_prod_order.store_id = int_store.store_id
        left join int_products int_product on agg_prod_order.product_id = int_product.product_id
)

select * from measures_and_dimension_info