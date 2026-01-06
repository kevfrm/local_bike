with int_orders as (

    select * from {{ ref('int_sales__orders') }}

),

int_stores as (

    select * from {{ ref('int_sales__stores') }}

),

int_staffs as (

    select * from {{ ref('int_sales__staffs') }}

),

aggregate_orders_info as (
    
    select
        int_order.order_date as reporting_date,
        int_order.store_id as store_id,
        int_order.staff_id as staff_id,
        round(sum(int_order.total_order_amount), 2) as total_order_amount,
        round(avg(int_order.total_order_amount), 2) as average_order_amount,
        count(distinct int_order.order_id) as total_orders
    from 
        int_orders int_order
    group by
        reporting_date,
        store_id,
        staff_id
),

measures_and_dimension_info as (
    
    select
        agg_order.reporting_date as reporting_date,
        int_store.store_name as store_name,
        int_store.store_street as store_street,
        int_store.store_city as store_city,
        int_store.store_zip_code as store_zip_code,
        int_staff.staff_name as staff_name,
        agg_order.total_order_amount as total_order_amount,
        agg_order.average_order_amount as average_order_amount,
        agg_order.total_orders as total_orders
    from 
        aggregate_orders_info agg_order left join int_stores int_store on agg_order.store_id = int_store.store_id
        left join int_staffs int_staff on agg_order.staff_id = int_staff.staff_id
)

select * from measures_and_dimension_info
