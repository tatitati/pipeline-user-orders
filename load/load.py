#!/usr/local/bin/python3
import configparser
import snowflake.connector

parser = configparser.ConfigParser()
parser.read("../pipeline.conf")
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

sql_users = """
        COPY INTO "MYDBT"."DE_BRONZE".users
        from (
          select *, current_timestamp(), concat('s3://pipelineusersorders/',METADATA$FILENAME), METADATA$FILE_ROW_NUMBER 
          from @s3pipelineusersorders/users
        )
        pattern = '.*/.*[.]parquet'
        file_format = (type=PARQUET COMPRESSION=SNAPPY);
      """

sql_orders= """
        COPY INTO "MYDBT"."DE_BRONZE".orders
        from (
          select *, current_timestamp(), concat('s3://pipelineusersorders/',METADATA$FILENAME), METADATA$FILE_ROW_NUMBER 
          from @s3pipelineusersorders/orders
        )
        pattern = '.*/.*[.]parquet'
        file_format = (type=PARQUET COMPRESSION=SNAPPY);
      """

cur = snow_conn.cursor()
cur.execute(sql_users)
cur.execute(sql_orders)
cur.close()