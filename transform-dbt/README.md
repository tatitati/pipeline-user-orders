```
dbt run -m model_bronze
dbt run -m model_silver

# incremental materialization
dbt run -m model_gold_dimensions
dbt snapshot # this give us the dimensions with SCD-2
dbt run -m model_gold_facts
```