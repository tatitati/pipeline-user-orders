#!/usr/local/bin/python3
from io import BytesIO
import boto3
from pyspark.sql import SparkSession
from pyspark import SparkContext
import configparser
import datetime
import os
import snowflake.connector

parser = configparser.ConfigParser()
parser.read("../pipeline.conf")
username = parser.get("snowflake_credentials", "username")
password = parser.get("snowflake_credentials", "password")
account_name = parser.get("snowflake_credentials", "account_name")

snow_conn = snowflake.connector.connect(
    user = username,
    password = password,
    account = account_name
)

sql = """
        COPY INTO "MYDBT"."DE_BRONZE".raw_table
        from (
          select *,current_timestamp() 
          from @s3pipelineusersorders/2021-11-4/10_9_45.parquet
        )
        file_format = (type=PARQUET COMPRESSION=SNAPPY);
      """