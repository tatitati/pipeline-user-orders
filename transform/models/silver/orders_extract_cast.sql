{{ config(materialized='incremental', unique_key='order_id') }}

select
    parquet_raw:id::Integer as order_id,
    parquet_raw:id_user::Integer as id_user,
    parquet_raw:spent::Integer as spent,
    parquet_raw:status::String as status,
    parquet_raw:created_at::TIMESTAMP as ordered_at,
    parquet_raw:updated_at::TIMESTAMP as updated_at,
    created_at::TIMESTAMP as copied_at,
    CURRENT_TIMESTAMP as dbt_at
from {{ source('bronze', 'orders') }}
where created_at <= CURRENT_TIMESTAMP

{% if is_incremental() %}
  and created_at > (select coalesce(max(copied_at), '1900-01-01 00:00:00') from {{ this }})
{% endif %}