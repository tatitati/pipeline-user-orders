-- set UTC timezone
alter account set TIMEZONE = 'Europe/London';a

CREATE DATABASE mydbt;
CREATE SCHEMA "MYDBT"."DE_BRONZE";
CREATE SCHEMA "MYDBT"."DE_SILVER";
CREATE SCHEMA "MYDBT"."DE_GOLD";

CREATE STAGE "MYDBT"."DE_BRONZE".s3pipelineusersorders
    URL = 's3://pipelineusersorders'
    CREDENTIALS = (AWS_KEY_ID = 'XXXXXXXXXX' AWS_SECRET_KEY = 'XXXXXX');

CREATE OR REPLACE FILE FORMAT my_parquet_format
  TYPE = PARQUET
  COMPRESSION = SNAPPY;

CREATE OR REPLACE TABLE "MYDBT"."DE_BRONZE"."USERS"(
  PARQUET_RAW VARIANT not null,
  md5 varchar(100) not null,
  created_at datetime not null default CURRENT_TIMESTAMP(),
  source varchar not null,
  metadata_row_number integer not null
);

CREATE OR REPLACE TABLE "MYDBT"."DE_BRONZE"."ORDERS"(
  PARQUET_RAW VARIANT not null,
  md5 varchar(100) not null,
  created_at datetime not null default CURRENT_TIMESTAMP(),
  source varchar not null,
  metadata_row_number integer not null
);


create or replace pipe mydbt.de_bronze.users auto_ingest=true as
        COPY INTO "MYDBT"."DE_BRONZE"."USERS"
        from (
          select
            *,
            md5(*), -- to manage duplicates
            current_timestamp(),
            concat('s3://pipelineusersorders/',METADATA$FILENAME),
            METADATA$FILE_ROW_NUMBER
          from @"s3pipelineusersorders"/users
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
          from @"s3pipelineusersorders"/orders
        )
        pattern = '.*/.*[.]parquet'
        file_format = (type=PARQUET COMPRESSION=SNAPPY);


