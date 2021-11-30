
{% snapshot snapshot_incremental_dim_user %}
{{
    config(
        target_database="DBT_SOLUTION",
        target_schema="DE_GOLD",
        unique_key="id",

        strategy="check",
        check_cols=["id", "updated_at"]
    )
}}

select * from {{ source('source_gold', 'incremental_dim_users') }}
{% endsnapshot %}