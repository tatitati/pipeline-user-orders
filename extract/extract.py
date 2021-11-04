#!/usr/local/bin/python3
from io import BytesIO
import boto3
from pyspark.sql import SparkSession
from pyspark import SparkContext
import configparser
import datetime
import os
import snowflake.connector
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/tati/lab/de/pipeline-user-orders/mysql-connector-java-8.0.12/mysql-connector-java-8.0.12.jar  pyspark-shell'

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")
oltp_username = parser.get("oltp_users", "username")
oltp_password = parser.get("oltp_users", "password")
snowflake_username = parser.get("snowflake_credentials", "username")
snowflake_password = parser.get("snowflake_credentials", "password")
snowflake_account_name = parser.get("snowflake_credentials", "account_name")

snow_conn = snowflake.connector.connect(
    user = snowflake_username,
    password = snowflake_password,
    account = snowflake_account_name,
    database="mydbt",
    schema="de_bronze"
)

context = SparkContext(master="local[*]", appName="readJSON")


spark = SparkSession\
    .builder\
    .master("local")\
    .appName("PySpark_MySQL_test")\
    .getOrCreate()

df_users = spark\
    .read\
    .format("jdbc")\
    .option("url", "jdbc:mysql://localhost:3306/usersorders") \
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("query", "select * from users")\
    .option("user", oltp_username)\
    .option("password", oltp_password)\
    .load()

df_users.show()
# +------+
# |  name|
# +------+
# |samuel|
# +------+
# df_users.write.parquet("extract/users.parquet")

df_orders = spark\
    .read\
    .format("jdbc")\
    .option("url", "jdbc:mysql://localhost:3306/usersorders") \
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("dbtable", "orders")\
    .option("user", oltp_username)\
    .option("password", oltp_password)\
    .load()

df_orders.show()
# +---+-------+-----+-------------------+
# | id|id_user|spent|         created_at|
# +---+-------+-----+-------------------+
# |  1|      1|   30|2021-11-02 13:49:36|
# |  2|      1|   20|2021-11-02 13:49:36|
# |  3|      3|  100|2021-11-02 13:49:36|
# +---+-------+-----+-------------------+
# df_orders.write.parquet("extract/orders.parquet")


s3 = boto3.resource(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# upload users to s3
out_buffer = BytesIO()
df_users.toPandas().to_parquet(out_buffer, engine="auto", compression='snappy')
now = datetime.datetime.now()
s3_file = f'users/{now.year}-{now.month}-{now.day}/{now.hour}_{now.minute}_{now.second}.parquet'
s3.Object(bucket_name, s3_file).put(Body=out_buffer.getvalue())

# upload orders to s3
out_buffer = BytesIO()
df_orders.toPandas().to_parquet(out_buffer, engine="auto", compression='snappy')
now = datetime.datetime.now()
s3_file = f'orders/{now.year}-{now.month}-{now.day}/{now.hour}_{now.minute}_{now.second}.parquet'
s3.Object(bucket_name, s3_file).put(Body=out_buffer.getvalue())
