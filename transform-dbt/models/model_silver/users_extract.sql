select
    parquet_raw:id::number as id,
    parquet_raw:address::varchar as address,
    parquet_raw:age::number as age,
    parquet_raw:name::varchar as name,
    parquet_raw:created_at::datetime as created_at,
    parquet_raw:updated_at::datetime as updated_at
from {{ source('dbt_solution__source_bronze', 'USERS_CURRENT') }}
