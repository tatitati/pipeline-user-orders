#!/usr/local/bin/python3
from io import BytesIO
import boto3
from pyspark.sql import SparkSession
from pyspark import SparkContext
import configparser
import datetime
import os
import snowflake.connector
from pyspark.sql.functions import col, lit
from snowflake.connector import ProgrammingError

jarPath='/Users/tati/lab/de/pipeline-user-orders/jars'
jars = [
    # spark-mysql
    f'{jarPath}/spark-mysql/mysql-connector-java-8.0.12.jar',
    # spark-snowflake
    f'{jarPath}/spark-snowflake/snowflake-jdbc-3.13.10.jar',
    f'{jarPath}/spark-snowflake/spark-snowflake_2.12-2.9.2-spark_3.1.jar', # scala 2.12 + pyspark 3.1
]
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {",".join(jars)}  pyspark-shell'

parser = configparser.ConfigParser()
parser.read("../pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")
oltp_username = parser.get("oltp_users", "username")
oltp_password = parser.get("oltp_users", "password")
snowflake_username = parser.get("snowflake_credentials", "username")
snowflake_password = parser.get("snowflake_credentials", "password")
snowflake_account_name = parser.get("snowflake_credentials", "account_name")

# get date of the last ingestion date from
users_last_ingestion: datetime.datetime = datetime.datetime(1000, 4, 13)
orders_last_ingestion: datetime.datetime = datetime.datetime(1000, 4, 13)

snow_conn = snowflake.connector.connect(
    user = snowflake_username,
    password = snowflake_password,
    account = snowflake_account_name,
    database="mydbt",
    schema="de_bronze"
)

try:
    users_sql = """
        select max(created_at) 
        from "MYDBT"."DE_BRONZE"."USERS";
    """

    orders_sql = """
        select max(created_at) 
        from "MYDBT"."DE_BRONZE"."ORDERS";
    """
    cur = snow_conn.cursor()
    cur.execute(users_sql)
    for (col1) in cur:
        if col1[0] != None:
            users_last_ingestion = col1[0]
        break;
    cur.close()

    cur = snow_conn.cursor()
    cur.execute(orders_sql)
    for (col1) in cur:
        if col1[0] != None:
            orders_last_ingestion = col1[0]
        break;
except ProgrammingError as e:
    print(e.msg)
finally:
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
    .option("query", f'select * from users where created_at > \'{ users_last_ingestion.strftime("%Y-%m-%d %H:%M:%S") }\' or updated_at > \'{ users_last_ingestion.strftime("%Y-%m-%d %H:%M:%S") }\'')\
    .option("user", oltp_username)\
    .option("password", oltp_password)\
    .load()

# CONVERTING TO PARQUET IS BUGGER regarding to timestamps, is generaring invalid timestamps, SO I CHANGING THE COLUMN TO STRING
df_users_new = df_users\
    .withColumn("New_Column", col('created_at').cast("String"))\
    .drop("created_at")\
    .withColumnRenamed("New_Column", "created_at")
df_users_new = df_users_new\
    .withColumn("New_Column", col('updated_at').cast("String"))\
    .drop("updated_at")\
    .withColumnRenamed("New_Column", "updated_at")
df_users_new.show()
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
    .option("query", f'select * from orders where created_at > \'{ orders_last_ingestion.strftime("%Y-%m-%d %H:%M:%S") }\' or updated_at > \'{ orders_last_ingestion.strftime("%Y-%m-%d %H:%M:%S") }\' ')\
    .option("user", oltp_username)\
    .option("password", oltp_password)\
    .load()

# CONVERTING TO PARQUET IS BUGGER, SO I CHANGING THE COLUMN TO STRING
df_orders_new = df_orders\
    .withColumn("New_Column", col('created_at').cast("String"))\
    .drop("created_at")\
    .withColumnRenamed("New_Column", "created_at")
df_orders_new = df_orders_new\
    .withColumn("New_Column", col('updated_at').cast("String"))\
    .drop("updated_at")\
    .withColumnRenamed("New_Column", "updated_at")

df_orders_new.show()
# +---+-------+-----+-------------------+
# | id|id_user|spent|         created_at|
# +---+-------+-----+-------------------+
# |  1|      1|   30|2021-11-02 13:49:36|
# |  2|      1|   20|2021-11-02 13:49:36|
# |  3|      3|  100|2021-11-02 13:49:36|
# +---+-------+-----+-------------------+

# save in snowflake: current_load stage
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = {
    "sfURL": f'{snowflake_account_name}.snowflakecomputing.com/',
    "sfUser": snowflake_username,
    "sfPassword": snowflake_password,
    "sfDatabase": "mydbt",
    "sfSchema": "de_bronze",
    "sfWarehouse": "COMPUTE_WH",
    "parallelism": "64"
}


# can I read?
# dfresult = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
#   .options(**sfOptions) \
#   .option("query",  "select 1 as my_num") \
#   .load()
#
# dfresult.show()
#
# # can I write?
if df_orders_new.count() > 0:
    df_orders_new.write\
        .format(SNOWFLAKE_SOURCE_NAME)\
        .options(**sfOptions)\
        .option("dbtable", "df_orders_new")\
        .mode("append")\
        .save()

# apply MINUS to deduplicate



# upload resulting MINUS to s3
s3 = boto3.resource(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# upload users to s3
now = datetime.datetime.now()
orders_filename=f'orders/{now.year}-{now.month}-{now.day}/{now.hour}_{now.minute}_{now.second}.parquet'
users_filename=f'users/{now.year}-{now.month}-{now.day}/{now.hour}_{now.minute}_{now.second}.parquet'

if df_users_new.count() > 0:
    now = datetime.datetime.now()

    out_buffer = BytesIO()
    df_users_new.toPandas().to_parquet(out_buffer, engine="auto", compression='snappy')
    s3\
        .Object(
            bucket_name,
            users_filename)\
        .put(Body=out_buffer.getvalue())

# upload orders to s3
if df_orders_new.count() > 0:
    out_buffer = BytesIO()
    df_orders_new.toPandas().to_parquet(out_buffer, engine="auto", compression='snappy')

    s3\
        .Object(
            bucket_name,
            orders_filename)\
        .put(Body=out_buffer.getvalue())
