-- set UTC timezone
alter account set TIMEZONE = 'Europe/London';a

CREATE DATABASE mydbt;
CREATE SCHEMA "MYDBT"."BRONZE";
CREATE SCHEMA "MYDBT"."SILVER";
CREATE SCHEMA "MYDBT"."GOLD";

CREATE STAGE "MYDBT"."DE_BRONZE".s3pipelineusersorders
    URL = 's3://pipelineusersorders'
    CREDENTIALS = (AWS_KEY_ID = 'XXXXXXXXXX' AWS_SECRET_KEY = 'XXXXXX');

CREATE OR REPLACE FILE FORMAT my_parquet_format
  TYPE = PARQUET
  COMPRESSION = SNAPPY;

create or replace pipe mydbt.de_bronze.users auto_ingest=true as
        COPY INTO "MYDBT"."DE_BRONZE"."USERS"
        from (
          select
            *,
            md5(*), -- to manage duplicates
            current_timestamp(),
            concat('s3://pipelineusersorders/',METADATA$FILENAME),
            METADATA$FILE_ROW_NUMBER
          from @s3pipelineusersorders/USERS
        )
        pattern = '.*/.*[.]parquet'
        file_format = (type=PARQUET COMPRESSION=SNAPPY);

create or replace pipe mydbt.de_bronze.orders auto_ingest=true as
        COPY INTO "MYDBT"."DE_BRONZE"."ORDERS"
        from (
          select
            *,
            md5(*), -- to manage duplicates
            current_timestamp(),
            concat('s3://pipelineusersorders/',METADATA$FILENAME),
            METADATA$FILE_ROW_NUMBER
          from @s3pipelineusersorders/ORDERS
        )
        pattern = '.*/.*[.]parquet'
        file_format = (type=PARQUET COMPRESSION=SNAPPY);


