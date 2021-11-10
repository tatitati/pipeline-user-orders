BEGIN;
  merge into "MYDBT"."DE_SILVER"."ORDERS_EXTRACT_CAST" oec
    using (
        select
          parquet_raw:id::Integer as order_id,
          parquet_raw:created_at::TIMESTAMP as ordered_at,
          parquet_raw:id_user::Integer as id_user,
          parquet_raw:spent::Integer as spent,
          parquet_raw:status::String as status,
          CURRENT_TIMESTAMP as updated_at,
          CURRENT_TIMESTAMP as copied_at,
          CURRENT_TIMESTAMP as dbt_at
      from "MYDBT"."DE_BRONZE"."ORDERS"
      where
          created_at <= CURRENT_TIMESTAMP and
          created_at > (select coalesce(max(copied_at), '1900-01-01 00:00:00') from "MYDBT"."DE_SILVER"."ORDERS_EXTRACT_CAST")
      ) s
      on s.ORDER_ID = oec.order_id
      when matched then
        update set
            oec.order_id=s.order_id,
            oec.id_user=s.id_user,
            oec.spent=s.spent,
            oec.status=s.status,
            oec.ordered_at=s.ordered_at,
            oec.updated_at=s.updated_at,
            oec.copied_at=s.copied_at,
            oec.dbt_at=s.dbt_at
      when not matched then
        insert (order_id, id_user, spent, status, ordered_at, updated_at, copied_at, dbt_at)
        values(s.order_id, s.id_user, s.spent, s.status, s.ordered_at, s.updated_at, s.copied_at, s.dbt_at);

COMMIT;

-- select * from stream_orders_extract_cast__status;
-- select * from stream_orders_extract_cast;