{{ config(materialized='incremental') }}


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


{% if is_incremental() %}
    where copied_at > (select max(copied_at) from {{ this }})
{% endif %}