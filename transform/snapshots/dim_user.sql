{% snapshot dim_user %}
    {{
        config(
            target_database="mydbt",
            target_schema="de_gold",
            unique_key="user_id",
            strategy="timestamp",
            updated_at="updated_at"
        )
    }}

    --s Pro-Tip: Use sources in snapshots!
    select * from {{ source('silver', 'users_extract_cast') }}
{% endsnapshot %}