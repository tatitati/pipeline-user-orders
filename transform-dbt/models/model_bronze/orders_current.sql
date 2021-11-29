{{ config(materialized='table') }}

select
    parquet_raw as parquet_raw,
    md5 as md5,
    created_at as created_at,
    source as source
from {{ source('mydbt__source_bronze', 'STREAM_ORDERS') }}
