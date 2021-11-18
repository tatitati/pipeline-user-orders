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

tables = [
    ['ORDERS', 'STREAM_ORDERS', 'ORDERS_CURRENT'],
    ['USERS',  'STREAM_USERS',  'ORDERS_CURRENT']
]

for listTables in tables:
    cur.execute(f'truncate table if exists "MYDBT"."DE_SILVER"."{listTables[2]}";')  # truncate "current" table if exists
    cur.execute(f'create table if not exists "MYDBT"."DE_SILVER"."{listTables[2]}" like "MYDBT"."DE_BRONZE"."{listTables[0]}";') # create "current" table if needed

    current_sql = f"""
        insert into "MYDBT"."DE_SILVER"."{listTables[2]}"(parquet_raw, md5, created_at, source, METADATA_ROW_NUMBER)
            select parquet_raw, md5, created_at, source, METADATA_ROW_NUMBER
            from {listTables[1]};
    """
    print(current_sql)
    cur.execute(current_sql)

cur.close()