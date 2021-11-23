select
    parquet_raw:id::number as id,
    parquet_raw:id_user::number as id_user,
    parquet_raw:spent::number as spent,
    parquet_raw:status::varchar as status,
    parquet_raw:created_at::datetime as created_at
from {{ source('source_bronze', 'STREAM_ORDERS') }}
