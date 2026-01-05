with

source as (

    select * from {{ source('local_bike', 'products') }}

),

renamed as (

    select
        -- ids
        product_id,
        brand_id,
        category_id,

        -- strings
        product_name,

        -- numerics
        model_year as product_model_year,
        list_price as product_price

    from source

)

select * from renamed