#!/usr/local/bin/python3
from pyspark.sql import SparkSession
from pyspark import SparkContext
from urllib.request import Request, urlopen
from pyspark.sql.types import *
import boto3
import configparser
import datetime
import os

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")
oltp_username = parser.get("oltp_users", "username")
oltp_password = parser.get("oltp_users", "password")

countries = ["Spain", "India", "France", "Vietnam", "China"]
context = SparkContext(master="local[*]", appName="readJSON")
spark = SparkSession.builder.getOrCreate()
responsesAcc=[]

schema = StructType([
    StructField("main", StructType([
        StructField("temp", FloatType()),
        StructField("feels_like", FloatType()),
        StructField("temp_min", FloatType()),
        StructField("temp_max", FloatType()),
        StructField("pressure", FloatType()),
        StructField("humidity", FloatType())
    ])),
    StructField("id", IntegerType()),
    StructField("name", StringType())
])

for country in countries:
    url = f'http://api.openweathermap.org/data/2.5/weather?q={country}&appid={api_key}'
    print(url)

    # read json api
    httpData = urlopen(url).read().decode('utf-8')
    print(httpData)

    # convert to dataframe with an imposed schema to make sure that the structure is correct. We might do this as well with json-schemas (in json format)
    rdd = context.parallelize([httpData])
    jsonDF = spark.read.json(rdd, schema=schema)
    jsonDF.printSchema()
    # root
    #  |-- main: struct (nullable = true)
    #  |    |-- temp: float (nullable = true)
    #  |    |-- feels_like: float (nullable = true)
    #  |    |-- temp_min: float (nullable = true)
    #  |    |-- temp_max: float (nullable = true)
    #  |    |-- pressure: float (nullable = true)
    #  |    |-- humidity: float (nullable = true)
    #  |-- id: integer (nullable = true)
    #  |-- name: string (nullable = true)

    jsonDF.show()
    # +--------------------+-------+-----+
    # |                main|     id| name|
    # +--------------------+-------+-----+
    # |{282.57, 280.01, ...|2510769|Spain|
    # +--------------------+-------+-----+

    inJson = jsonDF.toJSON().first()
    responsesAcc.append(inJson)
    print(inJson)
    # {"main":{"temp":283.38,"feels_like":282.6,"temp_min":282.45,"temp_max":284.31,"pressure":1016.0,"humidity":82.0},"id":2510769,"name":"Spain"}

with open("extract/responses.json", 'a') as outfile1:
    for row in responsesAcc:
        outfile1.write(row + '\n')

s3 = boto3.resource(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key = secret_key
)

now = datetime.datetime.now()
s3_file = f'{now.year}-{now.month}-{now.day}/{now.hour}_{now.minute}_{now.second}.json'
s3.Bucket(bucket_name).upload_file("extract/responses.json", s3_file)

# clean
os.remove("extract/responses.json")