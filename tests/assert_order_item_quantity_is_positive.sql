select
    order_item_quantity
from {{ ref('stg_sales__order_item') }}
having order_item_quantity < 0