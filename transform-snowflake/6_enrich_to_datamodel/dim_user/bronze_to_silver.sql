-- our table "MYDBT"."DE_SILVER"."USERS_EXTRACT_CAST" has associated an stream, so we extract fields, cast and merge into this table.
-- After merging into this table our streams will show if is an update/insert/delete in an SCD-2 flavour
BEGIN;
    merge into "MYDBT"."DE_SILVER"."USERS_DEDUPLICATED" uec
        using (
                select
                  parquet_raw:id::Integer as user_id,
                  parquet_raw:address::String as address,
                  parquet_raw:age::Integer as age,
                  parquet_raw:name::String as name,
                  parquet_raw:created_at::TIMESTAMP as registered_at,
                  parquet_raw:updated_at::TIMESTAMP as updated_at,
                  created_at::TIMESTAMP as copied_at,
                  CURRENT_TIMESTAMP as dbt_at
              from "MYDBT"."DE_BRONZE"."USERS"
              where
                  created_at <= CURRENT_TIMESTAMP and
                  created_at > (select coalesce(max(copied_at), '1900-01-01 00:00:00') from "MYDBT"."DE_SILVER"."USERS_DEDUPLICATED")
              ) s
          on s.USER_ID = uec.user_id
          when matched then
            update set
                uec.user_id=s.user_id,
                uec.address=s.address,
                uec.age=s.age,
                uec.name=s.name,
                uec.registered_at=s.registered_at,
                uec.updated_at=s.updated_at,
                uec.copied_at=s.copied_at,
                uec.dbt_at=s.dbt_at
          when not matched then
            insert (user_id, address, age, name, registered_at, updated_at, copied_at, dbt_at)
            values(s.user_id, s.address, s.age, s.name, s.registered_at, s.updated_at, s.copied_at, s.dbt_at);
COMMIT;