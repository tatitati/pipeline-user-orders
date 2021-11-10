BEGIN;
    merge into MYDBT.DE_GOLD.DIM_ORDERSTATUS dos
        using (
            select *
            from stream_ordersstatus_deduplicated
            where metadata$action='INSERT' -- on update we have one delete + one insert
        ) soec__s
        on soec__s.order_id = dos.order_id
        when matched then
            update set
                dos.IS_EFFECTIVE = FALSE
        when not matched then
            insert (order_id, status, valid_from, is_effective)
            values (soec__s.order_id, soec__s.status, current_timestamp(), TRUE);

    insert into "MYDBT"."DE_GOLD"."DIM_ORDERSTATUS"(order_id, status, valid_from, IS_EFFECTIVE)
        select order_id, status, current_timestamp, TRUE
        from  stream_ordersstatus_deduplicated
        where
              metadata$action='INSERT' and
              metadata$isupdate=TRUE;

COMMIT;