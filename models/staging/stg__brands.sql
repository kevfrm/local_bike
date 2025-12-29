with

source as (

    select * from {{ source('local_bike', 'brands') }}

),

renamed as (

    select
        -- ids
        brand_id,
        
        -- strings
        brand_name

    from source

)

select * from renamed