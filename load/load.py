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
parser.read("pipeline.conf")
username = parser.get("snowflake", "username")
password = parser.get("snowflake", "password")
account_name = parser.get("snowflake", "account_name")