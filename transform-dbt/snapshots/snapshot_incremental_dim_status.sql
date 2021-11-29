
{% snapshot dim_status %}
{{
    config(
        target_database="DBT_SOLUTION",
        target_schema="DE_GOLD",
        unique_key="status",

        strategy="check",
        check_cols=["status"]
    )
}}

select * from {{ source('source_gold', 'incremental_dim_status') }}
{% endsnapshot %}