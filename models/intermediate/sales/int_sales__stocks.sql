with stocks as (

    select * from {{ ref('stg_sales__stocks') }}

)

select * from stocks