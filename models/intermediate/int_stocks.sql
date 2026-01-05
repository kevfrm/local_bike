with stocks as (

    select * from {{ ref('stg_sales__stocks') }}

),

stores as (

    select * from {{ ref('stg_sales__stores') }}

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

get_detailed_info_at_store_level as (

    select 
        -- stock info
        stock.stock_quantity as stock_quantity,
        -- product info
        stock.product_id as product_id,
        product.product_name as product_name,
        product.product_price as product_price,
        product.brand_id as brand_id,
        brand.brand_name as brand_name,
        product.category_id as category_id,
        category.category_name as category_name,
        -- store info
        stock.store_id as store_id,
        store.store_name as store_name,
        store.store_phone as store_phone,
        store.store_email as store_email,
        store.store_street as store_street,
        store.store_city as store_city,
        store.store_state as store_state,
        store.store_zip_code as store_zip_code        
    from
        stocks stock left join stores store on stock.store_id = store.store_id
        left join products product on stock.product_id = product.product_id
        left join brands brand on product.brand_id = brand.brand_id
        left join categories category on product.category_id = category.category_id
)

select * from get_detailed_info_at_store_level