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
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

snow_conn = snowflake.connector.connect(
    user = snowflake_username,
    password = snowflake_password,
    account = snowflake_account_name,
    database="mydbt",
    schema="de_silver")
cur = snow_conn.cursor()


# check dimension
cur.execute(f"""        
        create schema if not exists "MYDBT"."DATA_QUALITY"
    """)
cur.execute(f"""        
        create table if not exists "MYDBT"."DATA_QUALITY"."DIM_TESTS"(
            id number not null autoincrement primary key,
            description varchar not null,
            table_name varchar not null,
            test_sql varchar not null
         )                  
    """)
cur.execute(f"""
         create table if not exists "MYDBT"."DATA_QUALITY"."FACT_ERRORS"(
            id number not null autoincrement primary key,
            id_dim_tests number not null,
            amount_records_failing number not null,
            timestamp datetime not null default current_timestamp(),
            
            foreign key(id_dim_tests) references MYDBT.DATA_QUALITY.DIM_TESTS(id)            
         )                  
    """)

query = f"""
 insert overwrite into "MYDBT"."DATA_QUALITY"."DIM_TESTS"(id, description, table_name, test_sql)
    values
        (   
            1,
            'check USERS_EXTRACT_CAST table is not empty', 
            '"MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST"', 
            'select count(*) 
                from "MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST"'
        ),            
        (
            2,
            'check USERS_EXTRACT_CAST age is between 0 and 100',
            '"MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST"',
            'select *
                from "MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST" 
                where age < 0 or age > 100;'
        ),            
        (
            3,
            'check USERS_EXTRACT_CAST name is not empty',
            '"MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST"',
            'select *
                from "MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST" 
                where 
                name is null or name = \''\'';'
        ),            
        (
            4,
            'check ORDERS_EXTRACT_CAST spent is not null or negative',
            '"MYDBT"."DE_SILVER"."ORDERS_EXTRACT_CAST"',
            'select *
                from "MYDBT"."DE_SILVER"."ORDERS_EXTRACT_CAST" 
                where 
                spent = 0 or spent < 0;'
        ),    
        (
            5,
            'check ORDERS_EXTRACT_CAST status is valid',
            '"MYDBT"."DE_SILVER"."ORDERS_EXTRACT_CAST"',
            'select *
                from "MYDBT"."DE_SILVER"."ORDERS_EXTRACT_CAST" 
                where 
                status not in (\''adsfasdf\'', \''adsfasd\'', \''aa\'')'
        );                     
        """
cur.execute(query)

tables = ['USERS_EXTRACT_CAST', 'ORDERS_EXTRACT_CAST']
for table in tables:
    # get tests for each table
    cur.execute(f"""
        select id, test_sql
        from  "MYDBT"."DATA_QUALITY"."DIM_TESTS"
        where table_name = '"MYDBT"."DE_SILVER"."{table}"'
    """)
    tests = cur.fetchall()

    # run tests
    for test in tests:
        cur.execute(test[1])
        result = cur.fetchall()

        if len(result) == 0:
            print(f"passing")
        else:
            # we want one entry per failed test, not per row not passing the test
            query = f"""
                insert into  "MYDBT"."DATA_QUALITY"."FACT_ERRORS"(id_dim_tests, amount_records_failing)
                    values
                        ({test[0]}, {len(result)})
            """

            cur.execute(query)

cur.close()
