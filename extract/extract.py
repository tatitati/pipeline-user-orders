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

# get date of the last ingestion date
snow_conn = snowflake.connector.connect(
    user = snowflake_username,
    password = snowflake_password,
    account = snowflake_account_name,
    database="mydbt",
    schema="de_bronze"
)

users_sql = """
    select max(created_at) 
    from "MYDBT"."DE_BRONZE"."USERS";
"""

orders_sql = """
    select max(created_at) 
    from "MYDBT"."DE_BRONZE"."ORDERS";
"""
users_last_ingestion: datetime.datetime = None
orders_last_ingestion: datetime.datetime = None
cur = snow_conn.cursor()
cur.execute(users_sql)
for (col1) in cur:
    users_last_ingestion = col1[0]
    break;
cur.close()

cur = snow_conn.cursor()
cur.execute(orders_sql)
for (col1) in cur:
    orders_last_ingestion = col1[0]
    break;

cur.close()


# extract data only since the last ingestion date
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
    .option("query", f'select * from users where updated_at > { users_last_ingestion.timestamp() }')\
    .option("user", oltp_username)\
    .option("password", oltp_password)\
    .load()

df_users.show()
# +------+
# |  name|
# +------+
# |samuel|
# +------+

df_orders = spark\
    .read\
    .format("jdbc")\
    .option("url", "jdbc:mysql://localhost:3306/usersorders") \
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("query", f'select * from orders where updated_at > { orders_last_ingestion.timestamp() }')\
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

# upload to s3
s3 = boto3.resource(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# upload users to s3
if df_users.count() > 0:
    out_buffer = BytesIO()
    df_users.toPandas().to_parquet(out_buffer, engine="auto", compression='snappy')
    now = datetime.datetime.now()
    s3_file = f'users/{now.year}-{now.month}-{now.day}/{now.hour}_{now.minute}_{now.second}.parquet'
    s3.Object(bucket_name, s3_file).put(Body=out_buffer.getvalue())

# upload orders to s3
if df_orders.count() > 0:
    out_buffer = BytesIO()
    df_orders.toPandas().to_parquet(out_buffer, engine="auto", compression='snappy')
    now = datetime.datetime.now()
    s3_file = f'orders/{now.year}-{now.month}-{now.day}/{now.hour}_{now.minute}_{now.second}.parquet'
    s3.Object(bucket_name, s3_file).put(Body=out_buffer.getvalue())
