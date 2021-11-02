#!/usr/local/bin/python3
from pyspark.sql import SparkSession
from pyspark import SparkContext
from urllib.request import Request, urlopen
from pyspark.sql.types import *
import boto3
import configparser
import findspark
import datetime
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/tati/lab/de/pipeline-user-orders/mysql-connector-java-8.0.12/mysql-connector-java-8.0.12.jar  pyspark-shell'

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")
oltp_username = parser.get("oltp_users", "username")
oltp_password = parser.get("oltp_users", "password")

context = SparkContext(master="local[*]", appName="readJSON")


spark = SparkSession\
    .builder\
    .master("local")\
    .appName("PySpark_MySQL_test")\
    .getOrCreate()


conn_df = spark\
    .read\
    .format("jdbc")\
    .option("url", "jdbc:mysql://localhost:3306/usersorders") \
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("dbtable", "users")\
    .option("user", oltp_username)\
    .option("password", oltp_password)\
    .load()

conn_df.show()
# +---+---------+---+------------------+-------------------+----------+
# | id|     name|age|           address|         created_at|updated_at|
# +---+---------+---+------------------+-------------------+----------+
# |  1|francisco| 34|   chalk avenue 23|2021-11-02 13:49:36|      null|
# |  2|   samuel| 86|    crown road 101|2021-11-02 13:49:36|      null|
# |  3|     john| 20|salvador square 10|2021-11-02 13:49:36|      null|
# +---+---------+---+------------------+-------------------+----------+


