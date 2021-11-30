{{ config(materialized="incremental", unique_key = "id") }}

select
    {{ dbt_utils.surrogate_key(['id', 'address', 'age', 'name', 'created_at', 'updated_at']) }} as sk,
    *,
    current_timestamp() as dbt_at
from {{ source('source_silver', 'USERS_EXTRACT') }}
