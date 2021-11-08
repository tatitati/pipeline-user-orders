{{ config(materialized='incremental', unique_key='user_id') }}

select
    parquet_raw:id::Integer as user_id,
    parquet_raw:address::String as address,
    parquet_raw:age::Integer as age,
    parquet_raw:name::String as name,
    parquet_raw:created_at::TIMESTAMP as registered_at,
    parquet_raw:updated_at::TIMESTAMP as updated_at,
    created_at::TIMESTAMP as copied_at,
    CURRENT_TIMESTAMP as dbt_at
from {{ source('bronze', 'users') }}
where created_at <= CURRENT_TIMESTAMP

{% if is_incremental() %}
  and created_at > (select coalesce(max(copied_at), '1900-01-01 00:00:00') from {{ this }})
{% endif %}