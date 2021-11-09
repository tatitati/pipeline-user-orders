-- set UTC timezone
alter account set TIMEZONE = 'Europe/London';a

CREATE DATABASE mydbt;

--
-- schema bronze
--
CREATE SCHEMA "MYDBT"."BRONZE";
CREATE STAGE "MYDBT"."DE_BRONZE".s3pipelineusersorders
    URL = 's3://pipelineusersorders'
    CREDENTIALS = (AWS_KEY_ID = 'XXXXXXXXXX' AWS_SECRET_KEY = 'XXXXXX');

CREATE OR REPLACE FILE FORMAT my_parquet_format
  TYPE = PARQUET
  COMPRESSION = SNAPPY;

create or replace pipe mydbt.de_bronze.users auto_ingest=true as
        COPY INTO "MYDBT"."DE_BRONZE".users
        from (
          select
            *,
            current_timestamp(),
            concat('s3://pipelineusersorders/',METADATA$FILENAME),
            METADATA$FILE_ROW_NUMBER
          from @s3pipelineusersorders/users
        )
        pattern = '.*/.*[.]parquet'
        file_format = (type=PARQUET COMPRESSION=SNAPPY);

create or replace pipe mydbt.de_bronze.orders auto_ingest=true as
        COPY INTO "MYDBT"."DE_BRONZE".orders
        from (
          select
            *,
            current_timestamp(),
            concat('s3://pipelineusersorders/',METADATA$FILENAME),
            METADATA$FILE_ROW_NUMBER
          from @s3pipelineusersorders/orders
        )
        pattern = '.*/.*[.]parquet'
        file_format = (type=PARQUET COMPRESSION=SNAPPY);

CREATE OR REPLACE TABLE "MYDBT"."DE_BRONZE".users(
  PARQUET_RAW VARIANT not null,
  created_at datetime not null default CURRENT_TIMESTAMP(),
  filename varchar not null,
  metadata_row_number integer not null
);
CREATE OR REPLACE TABLE "MYDBT"."DE_BRONZE".orders(
  PARQUET_RAW VARIANT not null,
  created_at datetime not null default CURRENT_TIMESTAMP(),
  filename varchar not null,
  metadata_row_number integer not null
);

--
-- schema silver
--
CREATE SCHEMA "MYDBT"."SILVER";

CREATE OR REPLACE STREAM stream_orders_extract_cast__status ON TABLE "MYDBT"."DE_SILVER"."ORDERS_EXTRACT_CAST";
CREATE OR REPLACE STREAM stream_orders_extract_cast__status ON TABLE "MYDBT"."DE_SILVER"."ORDERS_EXTRACT_CAST";
CREATE OR REPLACE STREAM stream_users_extract_cast ON TABLE "MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST";

create or replace TABLE "MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST" (
	USER_ID NUMBER(38,0),
	ADDRESS VARCHAR(16777216),
	AGE NUMBER(38,0),
	NAME VARCHAR(16777216),
	REGISTERED_AT TIMESTAMP_NTZ(9),
  	UPDATED_AT TIMESTAMP_NTZ(9),
	COPIED_AT TIMESTAMP_NTZ(9),
	DBT_AT TIMESTAMP_LTZ(9)
);

create or replace TABLE "MYDBT"."DE_SILVER"."ORDERS_EXTRACT_CAST" (
	ORDER_ID NUMBER(38,0),
	ID_USER NUMBER(38,0),
	SPENT NUMBER(38,0),
	STATUS VARCHAR(16777216),
	ORDERED_AT TIMESTAMP_NTZ(9),
	UPDATED_AT TIMESTAMP_NTZ(9),
	COPIED_AT TIMESTAMP_NTZ(9),
	DBT_AT TIMESTAMP_LTZ(9)
);

--
-- schema gold
--
CREATE SCHEMA "MYDBT"."GOLD";

create or replace TABLE "MYDBT"."DE_GOLD"."DIM_USER" (
    id number not null autoincrement, -- surrogate key
	USER_ID NUMBER(38,0),
	ADDRESS VARCHAR(16777216),
	AGE NUMBER(38,0),
	NAME VARCHAR(16777216),
	REGISTERED_AT TIMESTAMP_NTZ(9),
	VALID_FROM TIMESTAMP_NTZ(9) default current_timestamp(),
    IS_EFFECTIVE BOOLEAN default TRUE
);

create or replace TABLE "MYDBT"."DE_GOLD"."DIM_ORDERSTATUS" (
    id number not null autoincrement, -- surrogate key
	ORDER_ID NUMBER(38,0),
	STATUS VARCHAR(16777216),
	VALID_FROM TIMESTAMP_NTZ(9) default current_timestamp(),
    IS_EFFECTIVE BOOLEAN default TRUE
);
