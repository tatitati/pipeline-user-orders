{{ config(materialized="incremental", unique_key = "id") }}

select
    orders_extract_cast.id,
    orders_extract_cast.created_at,
    orders_extract_cast.spent,
    dim_users.sk as id_user,
    dim_status.sk as id_status
from {{ source('source_silver', 'ORDERS_EXTRACT') }} orders_extract_cast
left join {{ source('snapshot', 'snapshot_incremental_dim_user') }} dim_users on dim_users.id=orders_extract_cast.id_user
left join {{ source('snapshot', 'snapshot_incremental_dim_status') }} dim_status on dim_status.id_order=orders_extract_cast.id
where dim_status.status='processing' -- only new orders are inserted in facts. The rest are inserted into dimensions only with the SCD-2 updates
