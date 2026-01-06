with int_stocks as (

    select * from {{ ref('int_sales__stocks') }}

),

int_stores as (

    select * from {{ ref('int_sales__stores') }}

),

int_products as (

    select * from {{ ref('int_sales__products') }}

),

measures_and_dimension_info as (
    
    select
        int_store.store_name as store_name,
        int_store.store_phone as store_phone,
        int_store.store_email as store_email,
        int_store.store_street as store_street,
        int_store.store_city as store_city,
        int_store.store_state as store_state,
        int_store.store_zip_code as store_zip_code,
        int_product.product_name as product_name,
        int_product.product_brand as product_brand,
        int_product.product_category as product_category,
        int_stock.stock_quantity as stock_quantity
    from 
        int_stocks int_stock left join int_stores int_store on int_stock.store_id = int_store.store_id
        left join int_products int_product on int_stock.product_id = int_product.product_id

)

select * from measures_and_dimension_info