with customers as (

    select * from {{ ref('stg_sales__customers') }}

)

select * from customers