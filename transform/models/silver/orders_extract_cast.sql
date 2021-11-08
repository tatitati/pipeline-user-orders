select
    parquet_raw:id::Integer as order_id,
    parquet_raw:id_user::Integer as id_user,
    parquet_raw:spent::Integer as spent,
    parquet_raw:status::String as status,
    parquet_raw:created_at::TIMESTAMP as created_at,
    parquet_raw:updated_at::TIMESTAMP as updated_at
from {{ source('bronze', 'orders') }}