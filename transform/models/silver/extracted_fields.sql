select
    parquet_raw:address as address,
    parquet_raw:age as age,
    parquet_raw:name as name
from {{ source('debronze_rawtable', 'raw_table') }}