-- set UTC timezone
alter account set TIMEZONE = 'Europe/London';a

create database dbt_solution;
create schema bronze;
create schema silver;
create schema gold;

CREATE STAGE "DBT_SOLUTION"."BRONZE".s3pipelineusersorders
    URL = 's3://pipelineusersorders'
    CREDENTIALS = (AWS_KEY_ID = 'XXXXXXXXXX' AWS_SECRET_KEY = 'XXXXXX');

CREATE OR REPLACE FILE FORMAT my_parquet_format
  TYPE = PARQUET
  COMPRESSION = SNAPPY;

create or replace pipe dbt_solution.bronze.users auto_ingest=true as
        COPY INTO "DBT_SOLUTION"."BRONZE"."USERS"
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

create or replace pipe dbt_solution.bronze.orders auto_ingest=true as
        COPY INTO "DBT_SOLUTION"."BRONZE"."ORDERS"
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


