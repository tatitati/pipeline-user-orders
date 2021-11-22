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
import sys
import snowflake.connector
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, TimestampType, StringType
from snowflake.connector import ProgrammingError



# example use:
# /usr/local/bin/python3.6 /Users/tati/lab/de/pipeline-user-orders/schema_manager/schema_add.py user_1.json
# /usr/local/bin/python3.6 /Users/tati/lab/de/pipeline-user-orders/schema_manager/schema_add.py order_1.json

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
parser.read("./pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")
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

# read schema
schema_filename = sys.argv[1]
file = open(f'schema_manager/schemas/{schema_filename}', mode='r')
schema_content = file.read()
file.close()


cur.execute(f"""
    create table if not exists "MYDBT"."DE_SILVER"."SCHEMA_EVOLUTION"(
        id number not null autoincrement  primary key,
        source varchar not null comment 'this field specify for which table (containing raw values) the schema applies to',
        filename varchar not null default '{schema_filename}',
        schema_definition varchar not null,
        effective_from datetime not null default current_timestamp,
        is_effective boolean not null default true
    );
""")

s3 = boto3.resource(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)
bucket = s3.Bucket(bucket_name)

entity, version_and_extension = schema_filename.split("_")


now = datetime.datetime.now()
filename = f'schemas/{entity}/{schema_filename}'


print("uploading schema to s3:")
bucket.put_object(Key=filename, Body=schema_content)



# trigger COPY into ?