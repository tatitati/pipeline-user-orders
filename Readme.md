## learning

### Q: should I use csv or parquet?

Parquet:
    -> it defines an schema (faster to read data)
    -> allow schema evolution
    -> includes data compression
    -> etc

### Q: can I copy parquet files from s3 to snowflake?
    -> yes

### Q: do I need to specify the schema when reading from oltp-mysql?
    -> No. If you’re loading data into Spark from a file, you’ll probably want to specify a schema to avoid making Spark infer it. For a MySQL database, however, that’s not necessary since it has its own schema and Spark can translate it.

### Q: I uploaded a parquet file to s3 (myfile.parquet). How can I see from snowflake what data contains this?:

    ```
    CREATE STAGE "MYDBT"."DE_BRONZE".s3pipelineusersorders
    URL = 's3://pipelineusersorders'
    CREDENTIALS = (AWS_KEY_ID = 'XXXXX' AWS_SECRET_KEY = 'XXXXXXX');

    LIST @s3pipelineusersorders;

    CREATE OR REPLACE FILE FORMAT my_parquet_format
      TYPE = PARQUET
      COMPRESSION = SNAPPY;

    SELECT $1
    FROM @s3pipelineusersorders/myfile.parquet
    (file_format => 'my_parquet_format');

    SELECT $1:name::varchar
    FROM @s3pipelineusersorders/myfile.parquet
    (file_format => 'my_parquet_format');
    ```

### Q: how do I init the dbt folder named transform?:
    -> dbt init transform