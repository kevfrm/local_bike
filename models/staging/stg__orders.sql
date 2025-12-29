with

source as (

    select * from {{ source('local_bike', 'orders') }}

),

renamed as (

    select
        -- ids
        order_id,
        customer_id,
        store_id,
        staff_id,

        -- numerics
        order_status,

        -- date
        order_date,
        required_date as order_required_date,
        case when shipped_date = 'NULL' then null else shipped_date end as order_shipped_date

    from source

)

select * from renamed