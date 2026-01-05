with int_orders as (

    select * from {{ ref('int_orders') }}

),

aggregate_orders_info as (
    
    select
        int_order.order_date as reporting_date,
        int_order.store_name as store_name,
        int_order.store_street as store_street,
        int_order.store_city as store_city,
        int_order.store_zip_code as store_zip_code,
        int_order.staff_name as staff_name,
        round(sum(int_order.total_order_amount), 2) as total_order_amount,
        round(avg(int_order.total_order_amount), 2) as average_order_amount,
        count(distinct int_order.order_id) as total_orders
    from 
        int_orders int_order
    group by
        reporting_date,
        store_name,
        store_street,
        store_city,
        store_zip_code,
        staff_name
)

select * from aggregate_orders_info
