with

source as (

    select * from {{ source('local_bike', 'categories') }}

),

renamed as (

    select
        -- ids
        category_id,
        
        -- strings
        category_name

    from source

)

select * from renamed