--
-- extract, cast and deduplicate
--
insert into MYDBT.DE_SILVER.ORDERS_EXTRACT_CAST(order_id, id_user, spent, status, ORDERED_AT, updated_at, copied_at, dbt_at, md5)
    select
        order_id,
        id_user,
        spent,
        status,
        ordered_at,
        updated_at,
        copied_at,
        dbt_at,
        md5
    from (
         select
          parquet_raw:id::Integer as order_id,
          parquet_raw:id_user::Integer as id_user,
          parquet_raw:spent::Integer as spent,
          parquet_raw:status::String as status,
          parquet_raw:created_at::TIMESTAMP as ordered_at,
          CURRENT_TIMESTAMP as updated_at,
          CURRENT_TIMESTAMP as copied_at,
          CURRENT_TIMESTAMP as dbt_at,
          md5,
          row_number() over (partition by md5 order by md5) as duplicates
        from MYDBT.DE_BRONZE.ORDERS
        where
            created_at < current_timestamp and
            created_at >= (select coalesce(max(copied_at), '1900-01-01 00:00:00') from MYDBT.DE_SILVER.ORDERS_EXTRACT_CAST)
     )
    where duplicates = 1;

--
-- deduplicate (ORDERS_DEDUPLICATE HAS AN STREAM)
--
merge into MYDBT.DE_SILVER.ORDERS_DEDUPLICATED od
    using (
        select order_id,
               id_user,
               spent,
               status,
               ordered_at,
               updated_at,
               copied_at,
               dbt_at,
               md5
        from (
                 select *,
                        row_number() over (partition by md5 order by md5) as duplicates
                 from MYDBT.DE_SILVER.ORDERS_EXTRACT_CAST
                 where copied_at < current_timestamp
                   and copied_at >=
                       (select coalesce(max(copied_at), '1900-01-01 00:00:00') from MYDBT.DE_SILVER.ORDERS_DEDUPLICATED)
             )
    ) oec
    on oec.ORDER_ID = od.ORDER_ID
    when matched then
            update set
                od.order_id=oec.order_id,
                od.id_user=oec.id_user,
                od.spent=oec.spent,
                od.status=oec.status,
                od.ordered_at=oec.ordered_at,
                od.updated_at=oec.updated_at,
                od.copied_at=oec.copied_at,
                od.dbt_at=oec.dbt_at,
                od.MD5=oec.md5
      when not matched then
        insert (order_id, id_user, spent, status, ordered_at, updated_at, copied_at, dbt_at, md5)
        values(oec.order_id, oec.id_user, oec.spent, oec.status, oec.ordered_at, oec.updated_at, oec.copied_at, oec.dbt_at, oec.MD5);






