{% snapshot dim_orderstatus %}
    {{
        config(
            target_database="mydbt",
            target_schema="de_gold",
            unique_key="order_id",
            strategy="timestamp",
            updated_at="updated_at"
        )
    }}

    --s Pro-Tip: Use sources in snapshots!
    select
        order_id,
        status,
        updated_at
    from {{ source('silver', 'orders_extract_cast') }}
{% endsnapshot %}