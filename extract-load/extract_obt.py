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
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, TimestampType, StringType
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
last_ingestion: datetime.datetime = datetime.datetime(1000, 4, 13)

context = SparkContext(master="local[*]", appName="readJSON")
spark = SparkSession.builder.master("local").appName("PySpark_MySQL_test").getOrCreate()
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = {
    "sfURL": f'{snowflake_account_name}.snowflakecomputing.com/',
    "sfUser": snowflake_username,
    "sfPassword": snowflake_password,
    "sfDatabase": "MYDBT",
    "sfSchema": "DE_BRONZE",
    "sfWarehouse": "COMPUTE_WH",
    "parallelism": "64"
}
snow_conn = snowflake.connector.connect(
    user = snowflake_username,
    password = snowflake_password,
    account = snowflake_account_name,
    database="mydbt",
    schema="de_bronze"
)


try:
    max_created = f"""
        select max(created_at)
        from "MYDBT"."DE_BRONZE"."ORDERS";
    """

    cur = snow_conn.cursor()
    cur.execute(max_created)
    for (col1) in cur:
        if col1[0] != None:
            last_ingestion = col1[0]
        break;

except ProgrammingError as e:
    print(e.msg)

# i can use schemas stored in s3 for this indeed
schema_sales =  StructType([
                StructField("order_id", IntegerType(), False),
                StructField("order_id_user", IntegerType(), False),
                StructField("order_spent", IntegerType(), False),
                StructField("order_status", StringType(), False),
                StructField("order_created_at", TimestampType(), False),
                StructField("order_updated_at", TimestampType(), True),
                StructField("user_id", IntegerType(), False),
                StructField("user_name", StringType(), False),
                StructField("user_age", IntegerType(), False),
                StructField("user_address", StringType(), False),
                StructField("user_created_at", TimestampType(), False),
                StructField("user_updated_at", TimestampType(), True)
])


rdd_new = spark\
    .read\
    .format("jdbc")\
    .option("url", "jdbc:mysql://localhost:3306/usersorders") \
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("query", f"""
                        select 
                            o.id as order_id, 
                            o.id_user as order_id_user, 
                            o.spent as order_spent, 
                            o.status as order_status, 
                            o.created_at as order_created_at, 
                            o.updated_at as order_updated_at,

                            u.id as user_id,
                            u.name as user_name,
                            u.age as user_age,
                            u.address as user_address,
                            u.created_at as user_created_at,
                            u.updated_at as user_updated_at            
                        from orders o
                        left join users u on o.id_user = u.id
                        where 
                            o.created_at > '{ last_ingestion.strftime("%Y-%m-%d %H:%M:%S") }' or 
                            o.updated_at > '{ last_ingestion.strftime("%Y-%m-%d %H:%M:%S") }'
    """)\
    .option("user", oltp_username)\
    .option("password", oltp_password) \
    .load().rdd

# validate schema
df_new = spark.createDataFrame(rdd_new, schema_sales)

# CONVERTING TO PARQUET IS BUGGER regarding to timestamps, is generaring invalid timestamps, SO I CHANGING THE COLUMN TO STRING
for table in ['order', 'user']:
    df_new = df_new\
        .withColumn("New_Column", col(table +'_created_at').cast("String"))\
        .drop(table + "_created_at")\
        .withColumnRenamed("New_Column", table + "_created_at")

    df_new = df_new\
        .withColumn("New_Column", col(table + '_updated_at').cast("String"))\
        .drop(table + "_updated_at")\
        .withColumnRenamed("New_Column", table + "_updated_at")

df_new.show()

s3 = boto3.resource(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

now = datetime.datetime.now()

schema_version = '1_0'  # is a db....im hardcoding for now
schema_name = table.lower()
bucket = "s3://pipelineusersorders"
schema_url = f"{bucket}/sales/{schema_version}.json"
filename=f'sales/{schema_version}/{now.year}-{now.month}-{now.day}/{now.hour}_{now.minute}_{now.second}.parquet'

df_with_schema = df_new \
    .withColumn("schema_version", lit(schema_version)) \
    .withColumn("schema_name", lit('sales')) \
    .withColumn("schema_url", lit(schema_url))

print(df_with_schema.schema.json())

df_with_schema.show()

if df_with_schema.count() > 0:
    print(f'uploading to s3: {bucket}/{filename}')

    now = datetime.datetime.now()

    out_buffer = BytesIO()
    df_with_schema.toPandas().to_parquet(out_buffer, engine="auto", compression='snappy')
    s3\
        .Object(
            bucket_name,
            filename)\
        .put(Body=out_buffer.getvalue())


cur.close()

# sales
# +--------+-------------+-----------+------------+-------+---------+--------+------------------+-------------------+----------------+-------------------+---------------+
# |order_id|order_id_user|order_spent|order_status|user_id|user_name|user_age|      user_address|   order_created_at|order_updated_at|    user_created_at|user_updated_at|
# +--------+-------------+-----------+------------+-------+---------+--------+------------------+-------------------+----------------+-------------------+---------------+
# |       1|            1|         30|  processing|      1|francisco|      34|   chalk avenue 23|2021-12-09 18:42:44|            null|2021-12-09 18:42:44|           null|
# |       2|            1|         20|  processing|      1|francisco|      34|   chalk avenue 23|2021-12-09 18:42:44|            null|2021-12-09 18:42:44|           null|
# |       3|            3|        100|  processing|      3|     john|      20|salvador square 10|2021-12-09 18:42:44|            null|2021-12-09 18:42:44|           null|
# +--------+-------------+-----------+------------+-------+---------+--------+------------------+-------------------+----------------+-------------------+---------------+