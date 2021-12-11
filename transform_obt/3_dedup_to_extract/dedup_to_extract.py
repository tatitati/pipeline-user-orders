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
import json


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
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

snow_conn = snowflake.connector.connect(
    user = snowflake_username,
    password = snowflake_password,
    account = snowflake_account_name,
    database="mydbt",
    schema="de_silver")
cur = snow_conn.cursor()


# download all schemas from s3
s3_resource = boto3.resource(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)
my_bucket = s3_resource.Bucket(bucket_name)
schemas = {}
for object_summary in my_bucket.objects.filter(Prefix="schemas/"):
    if '.json' in object_summary.key:
        scheme, schema_name, schema_version = object_summary.key.split('/')
        body = object_summary.get()['Body'].read()
        if schema_name not in schemas:
            schemas[schema_name] = {}

        schemas[schema_name][schema_version] = body.decode('UTF-8')


schema_version = '1_0'

schema = schemas["sales"]['1_0.json']
schemajson = json.loads(schema)
columns_to_create = []
raw_fields = []

for i, field in enumerate(schemajson['fields']):
    nullControl = 'not null' if field['nullable'] == False else ''
    mapTypes = {
        "datetime": "timestamp",
        "string": "varchar",
        "integer": "number"
    }
    columns_to_create.append(f"{field['name']} {mapTypes[field['type']]} {nullControl}")
    raw_fields.append(f"parquet_raw:{field['name']}::{mapTypes[field['type']]}")

    if i == len(schemajson['fields']) - 1: # is last iteration?
        columns = ','.join(columns_to_create)
        parsers = ','.join(raw_fields)

        cur.execute(f"""
                 create or replace table "MYDBT"."DE_SILVER"."SALES_EXTRACT_CAST"(                        
                    {columns}                        
                 ) as
                    select
                        {parsers}
                    from
                        "MYDBT"."DE_SILVER"."SALES_DEDUP";
                """)

cur.close()