

CREATE OR REPLACE TABLE "MYDBT"."DE_BRONZE".users(
  PARQUET_RAW VARIANT not null,
  md5 varchar(100) not null,
  created_at datetime not null default CURRENT_TIMESTAMP(),
  source varchar not null,
  metadata_row_number integer not null
);

CREATE OR REPLACE TABLE "MYDBT"."DE_BRONZE".orders(
  PARQUET_RAW VARIANT not null,
  md5 varchar(100) not null,
  created_at datetime not null default CURRENT_TIMESTAMP(),
  source varchar not null,
  metadata_row_number integer not null
);

create or replace table orders_extract_current(
    id number not null,
    id_user number not null,
    spent number not null,
    status varchar not null,
    created_at datetime not null,
    updated_at datetime
);

create or replace table users_extract_current(
    id number not null,
    name varchar not null,
    age number not null,
    address varchar not null,
    created_at datetime not null,
    updated_at datetime
);

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

create table "MYDBT"."DE_SILVER"."ORDERS_DEDUPLICATED"(
    order_id number not null,
    id_user number not null,
    spent number not null,
    status varchar not null,
    ordered_at timestamp not null,
    updated_at timestamp not null,
    copied_at timestamp not null
);

create table "MYDBT"."DE_SILVER"."USERS_DEDUPLICATED"(
    user_id number not null,
    address varchar not null,
    age number not null,
    name varchar not null,
    registered_at timestamp not null,
    updated_at timestamp not null,
    copied_at timestamp not null
);

create or replace TABLE "MYDBT"."DE_GOLD"."DIM_USER" (
    id number not null autoincrement primary key , -- surrogate key
	USER_ID NUMBER(38,0),
	ADDRESS VARCHAR(16777216),
	AGE NUMBER(38,0),
	NAME VARCHAR(16777216),
	REGISTERED_AT TIMESTAMP_NTZ(9),
	VALID_FROM TIMESTAMP_NTZ(9) default current_timestamp(),
    IS_EFFECTIVE BOOLEAN default TRUE
);

create or replace table "MYDBT"."DE_GOLD"."DIM_ORDERSTATUS" (
      id number not null autoincrement primary key , -- surrogate key
      ORDER_ID number not null,
      status varchar(50) not null,
      valid_from datetime not null default current_timestamp(),
      is_effective boolean not null default TRUE
);

create or replace table MYDBT.DE_GOLD.FACT_ORDERS (
    id number not null autoincrement primary key, -- foreign key
    order_id number not null,
    orderstatus_id number not null,
    orderuser_id number not null,
    ordered_at datetime not null,

    foreign key (orderstatus_id) references MYDBT.DE_GOLD.DIM_ORDERSTATUS(id),
    foreign key (orderuser_id) references MYDBT.DE_GOLD.DIM_USER(id)
);

CREATE OR REPLACE STREAM stream_ordersstatus_deduplicated ON TABLE "MYDBT"."DE_SILVER"."ORDERS_DEDUPLICATED";
CREATE OR REPLACE STREAM stream_orders_deduplicated ON TABLE "MYDBT"."DE_SILVER"."ORDERS_DEDUPLICATED";
CREATE OR REPLACE STREAM stream_users_deduplicated ON TABLE "MYDBT"."DE_SILVER"."USERS_DEDUPLICATED";