

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

CREATE OR REPLACE STREAM stream_orders_extract_cast__status ON TABLE "MYDBT"."DE_SILVER"."ORDERS_EXTRACT_CAST";
CREATE OR REPLACE STREAM stream_orders_extract_cast ON TABLE "MYDBT"."DE_SILVER"."ORDERS_EXTRACT_CAST";
CREATE OR REPLACE STREAM stream_users_extract_cast ON TABLE "MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST";