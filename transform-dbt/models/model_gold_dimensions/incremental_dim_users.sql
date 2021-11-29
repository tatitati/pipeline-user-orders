{{ config(materialized="incremental", unique_key = "id") }}

select
    *,
    current_timestamp() as dbt_at
from {{ source('source_silver', 'USERS_EXTRACT') }}
