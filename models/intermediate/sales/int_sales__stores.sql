with stores as (

    select * from {{ ref('stg_sales__stores') }}

)

select * from stores