with int_stocks as (

    select * from {{ ref('int_stocks') }}

),

aggregate_stocks_info as (
    
    select
        sum(int_stock.stock_quantity) as stock_quantity,
        max(int_stock.product_name) as product_name,
        max(int_stock.brand_name) brand_name,
        max(int_stock.category_name) as category_name,
        max(int_stock.store_name) as store_name,
        max(int_stock.store_phone) as store_phone,
        max(int_stock.store_email) as store_email,
        max(int_stock.store_street) as store_street,
        max(int_stock.store_city) as store_city,
        max(int_stock.store_state) as store_state,
        max(int_stock.store_zip_code) as store_zip_code
    from 
        int_stocks int_stock
    group by
        int_stock.product_id,
        int_stock.store_id

)

select * from aggregate_stocks_info