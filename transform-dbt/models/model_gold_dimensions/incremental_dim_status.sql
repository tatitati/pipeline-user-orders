{{ config(materialized="incremental") }}

select
    {{ dbt_utils.surrogate_key(['id','status']) }} as sk,
    id as id_order,
    status,
    current_timestamp() as dbt_at
from {{ source('source_silver', 'ORDERS_EXTRACT') }}
