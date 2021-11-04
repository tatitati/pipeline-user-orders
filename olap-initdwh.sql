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