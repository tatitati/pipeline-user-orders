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

for table in ["USERS", "ORDERS"]:
    print(f'Entity: {table}')
    try:
        max_created = f"""
            select max(created_at)
            from "MYDBT"."DE_BRONZE".{table};
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
    schemas = {
        "USERS":
            StructType([
                    StructField("id", IntegerType(), False),
                    StructField("name", StringType(), False),
                    StructField("age", IntegerType(), False),
                    StructField("address", StringType(), False),
                    StructField("created_at", TimestampType(), False),
                    StructField("updated_at", TimestampType(), True)
            ]),
        "ORDERS":
            StructType([
                StructField("id", IntegerType(), False),
                StructField("id_user", IntegerType(), False),
                StructField("spent", IntegerType(), False),
                StructField("status", StringType(), False),
                StructField("created_at", TimestampType(), False),
                StructField("updated_at", TimestampType(), True)
            ]),
    }


    rdd_new = spark\
        .read\
        .format("jdbc")\
        .option("url", "jdbc:mysql://localhost:3306/usersorders") \
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .option("query", f"""select * from {table.lower()} where created_at > '{ last_ingestion.strftime("%Y-%m-%d %H:%M:%S") }' or updated_at > '{ last_ingestion.strftime("%Y-%m-%d %H:%M:%S") }'""")\
        .option("user", oltp_username)\
        .option("password", oltp_password) \
        .load().rdd

    # validate schema
    df_new = spark.createDataFrame(rdd_new, schemas[table])

    # CONVERTING TO PARQUET IS BUGGER regarding to timestamps, is generaring invalid timestamps, SO I CHANGING THE COLUMN TO STRING
    df_new_column = df_new\
        .withColumn("New_Column", col('created_at').cast("String"))\
        .drop("created_at")\
        .withColumnRenamed("New_Column", "created_at")
    df_new_column = df_new_column\
        .withColumn("New_Column", col('updated_at').cast("String"))\
        .drop("updated_at")\
        .withColumnRenamed("New_Column", "updated_at")

    s3 = boto3.resource(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    now = datetime.datetime.now()

    schema_version = '1_0'  # is a db....im hardcoding for now
    schema_name = table.lower()
    bucket = "s3://pipelineusersorders"
    schema_url = f"{bucket}/{schema_name}/{schema_version}.json"
    filename=f'{table.lower()}/{schema_version}/{now.year}-{now.month}-{now.day}/{now.hour}_{now.minute}_{now.second}.parquet'

    df_with_schema = df_new_column \
        .withColumn("schema_version", lit(schema_version)) \
        .withColumn("schema_name", lit(schema_name)) \
        .withColumn("schema_url", lit(schema_url))

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


# ORDERS:
# +---+-------+-----+----------+-------------------+----------+--------------+-----------+--------------------+--------------------+
# | id|id_user|spent|    status|         created_at|updated_at|schema_version|schema_name|          schema_url|            filename|
# +---+-------+-----+----------+-------------------+----------+--------------+-----------+--------------------+--------------------+
# |  1|      1|   30|processing|2021-12-08 20:02:42|      null|           1_0|     orders|s3://pipelineuser...|s3://pipelineuser...|
# |  2|      1|   20|processing|2021-12-08 20:02:42|      null|           1_0|     orders|s3://pipelineuser...|s3://pipelineuser...|
# |  3|      3|  100|processing|2021-12-08 20:02:42|      null|           1_0|     orders|s3://pipelineuser...|s3://pipelineuser...|
# +---+-------+-----+----------+-------------------+----------+--------------+-----------+--------------------+--------------------+

# USERS:
# +---+---------+---+------------------+-------------------+----------+--------------+-----------+--------------------+--------------------+
# | id|     name|age|           address|         created_at|updated_at|schema_version|schema_name|          schema_url|            filename|
# +---+---------+---+------------------+-------------------+----------+--------------+-----------+--------------------+--------------------+
# |  1|francisco| 34|   chalk avenue 23|2021-12-08 20:02:42|      null|           1_0|      users|s3://pipelineuser...|s3://pipelineuser...|
# |  2|   samuel| 86|    crown road 101|2021-12-08 20:02:42|      null|           1_0|      users|s3://pipelineuser...|s3://pipelineuser...|
# |  3|     john| 20|salvador square 10|2021-12-08 20:02:42|      null|           1_0|      users|s3://pipelineuser...|s3://pipelineuser...|
# +---+---------+---+------------------+-------------------+----------+--------------+-----------+--------------------+--------------------+
