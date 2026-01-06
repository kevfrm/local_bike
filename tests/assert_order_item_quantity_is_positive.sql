select
    order_item_id
from {{ ref('stg_sales__order_item') }}
where order_item_quantity < 0