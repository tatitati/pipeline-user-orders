select
    parquet_raw:address::String as address,
    parquet_raw:age::Integer as age,
    parquet_raw:name::String as name
from {{ source('debronze_rawtable', 'raw_table') }}