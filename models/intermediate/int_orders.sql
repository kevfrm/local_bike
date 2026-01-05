with orders as (

    select * from {{ ref('stg_sales__orders') }}

),

customers as (

    select * from {{ ref('stg_sales__customers') }}

),

stores as (

    select * from {{ ref('stg_sales__stores') }}

),

staffs as (

    select * from {{ ref('stg_sales__staffs') }}

),

order_items as (

    select * from {{ ref('stg_sales__order_items') }}

),

get_detailed_info_at_order_level as (

    select 
        -- order info
        s_order.order_id as order_id,
        s_order.order_status as order_status,
        s_order.order_date as order_date,
        round(sum(order_item.total_order_item_amount), 2) as total_order_amount,
        -- customer info
        concat(customer.customer_first_name, ' ', customer.customer_last_name) as customer_name,
        customer.customer_city as customer_city,
        customer.customer_state as customer_state,
        customer.customer_zip_code as customer_zip_code,
        -- store info
        s_order.store_id as store_id,
        store.store_name as store_name,
        store.store_street as store_street,
        store.store_city as store_city,
        store.store_zip_code as store_zip_code,
        -- staff info
        s_order.staff_id as staff_id,
        concat(staff.staff_first_name, ' ', staff.staff_last_name) as staff_name
    from
        orders s_order left join customers customer on s_order.customer_id = customer.customer_id
        left join stores store on s_order.store_id = store.store_id
        left join staffs staff on s_order.staff_id = staff.staff_id
        left join order_items order_item on s_order.order_id = order_item.order_id
    group by
      order_id,
      order_status,
      order_date,
      customer_name,
      customer_city,
      customer_state,
      customer_zip_code,
      store_id,
      store_name,
      store_street,
      store_city,
      store_zip_code,
      staff_id,
      staff_name
)

select * from get_detailed_info_at_order_level