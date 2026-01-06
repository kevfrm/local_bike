with products as (

    select * from {{ ref('stg_sales__products') }}

),

brands as (

    select * from {{ ref('stg_sales__brands') }}

),

categories as (

    select * from {{ ref('stg_sales__categories') }}

),

get_info_at_product_level as (

    select 
        -- product info
        product.product_id as product_id,
        product.product_name as product_name,
        product.product_price as product_price,
        -- brand info
        brand.brand_name as product_brand,
        -- category info
        category.category_name as product_category        
    from
        products product left join brands brand on product.brand_id = brand.brand_id
        left join categories category on product.category_id = category.category_id
)

select * from get_info_at_product_level