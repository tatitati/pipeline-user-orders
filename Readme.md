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