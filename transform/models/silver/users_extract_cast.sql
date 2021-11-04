select
    parquet_raw:address::String as address,
    parquet_raw:age::Integer as age,
    parquet_raw:name::String as name,
    parquet_raw:created_at::TIMESTAMP as created_at,
    parquet_raw:updated_at::TIMESTAMP as updated_at
from {{ source('debronze_users', 'users') }}