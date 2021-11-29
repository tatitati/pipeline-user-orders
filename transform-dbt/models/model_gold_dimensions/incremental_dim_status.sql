{{ config(materialized="incremental") }}

select
    id as id_order,
    status,
    current_timestamp() as dbt_at
from {{ source('source_silver', 'ORDERS_EXTRACT') }}
