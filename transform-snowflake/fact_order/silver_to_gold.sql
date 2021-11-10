insert into MYDBT.DE_GOLD.FACT_ORDERS(order_id, ORDERSTATUS_ID, ORDERUSER_ID, ORDERED_AT)
    select
           soec.order_id,
           dos.ID as id_dim_orderstatus,
           du.ID as id_dim_user,
           soec.ordered_at
    from stream_orders_extract_cast soec
    left join MYDBT.DE_GOLD.DIM_ORDERSTATUS dos on dos.order_id = soec.ORDER_ID
    left join MYDBT.DE_GOLD.DIM_USER du on du.USER_ID = soec.id_user
    where
        soec.status = 'processing' and
        metadata$action='INSERT' and
        metadata$isupdate=FALSE;

