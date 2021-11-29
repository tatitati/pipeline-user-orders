{{ config(materialized="incremental", unique_key = "id") }}

select
    orders_extract_cast.id,
    orders_extract_cast.created_at,
    orders_extract_cast.spent,
    dim_users.id as id_user,
    dim_status.id_order
from {{ source('source_silver', 'ORDERS_EXTRACT') }} orders_extract_cast
left join {{ source('snapshot', 'snapshot_incremental_dim_user') }} dim_users on dim_users.id=orders_extract_cast.id_user
left join {{ source('snapshot', 'snapshot_incremental_dim_status') }} dim_status on dim_status.id_order=orders_extract_cast.id
where dim_status.DBT_VALID_TO is null
