#!/usr/local/bin/python3

from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from io import BytesIO
import boto3
from pyspark.sql import SparkSession
from pyspark import SparkContext
import configparser
import datetime
import os
import snowflake.connector
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, TimestampType, StringType
from snowflake.connector import ProgrammingError

jarPath='/Users/tati/lab/de/pipeline-user-orders/jars'
jars = [
    # spark-snowflake
    f'{jarPath}/spark-snowflake/snowflake-jdbc-3.13.10.jar',
    f'{jarPath}/spark-snowflake/spark-snowflake_2.12-2.9.2-spark_3.1.jar', # scala 2.12 + pyspark 3.1
]
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {",".join(jars)}  pyspark-shell'
context = SparkContext(master="local[*]", appName="readJSON")
app = SparkSession.builder.appName("myapp").getOrCreate()

parser = configparser.ConfigParser()
parser.read("../../pipeline.conf")
snowflake_username = parser.get("snowflake_credentials", "username")
snowflake_password = parser.get("snowflake_credentials", "password")
snowflake_account_name = parser.get("snowflake_credentials", "account_name")
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

snow_conn = snowflake.connector.connect(
    user = snowflake_username,
    password = snowflake_password,
    account = snowflake_account_name,
    database="mydbt",
    schema="de_silver")
cur = snow_conn.cursor()


# USERS_DEDUP -> USERS_EXTRACT
cur.execute(f"""
         create or replace table "MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST"(
            id number not null,
            name varchar not null,
            address varchar not null,
            age number not null,
            created_at datetime not null,
            updated_at datetime
         ) as
            select 
                parquet_raw:id::number,
                parquet_raw:name::varchar,
                parquet_raw:address::varchar,
                parquet_raw:age::number,
                parquet_raw:created_at::datetime,
                parquet_raw:updated_at::datetime
            from 
                "MYDBT"."DE_SILVER"."USERS_DEDUP";    
        """)

# ORDERS_DEDUP -> ORDERS_EXTRACT
cur.execute(f"""
         create or replace table "MYDBT"."DE_SILVER"."ORDERS_EXTRACT_CAST"(
            id number not null,
            id_user number not null,
            spent number not null,
            status varchar not null,
            created_at datetime not null,
            updated_at datetime
         ) as
            select 
                parquet_raw:id::number,
                parquet_raw:id_user::number,
                parquet_raw:spent::number,
                parquet_raw:status::varchar,
                parquet_raw:created_at::datetime,
                parquet_raw:updated_at::datetime
            from 
                "MYDBT"."DE_SILVER"."ORDERS_DEDUP";    
        """)

cur.close()