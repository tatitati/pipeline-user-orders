BEGIN;
  merge into "MYDBT"."DE_GOLD"."DIM_USER" dim_user
    using (
      select *
      from stream_users_extract_cast
      where metadata$action='INSERT'
    ) s
    on s.USER_ID = dim_user.user_id
    when matched then
      update set
          dim_user.is_effective = FALSE
    when not matched then
      insert (user_id, address, age, name, registered_at, valid_from, is_effective)
      values(s.user_id, s.address, s.age, s.name, s.registered_at, current_timestamp(), TRUE);

  insert into "MYDBT"."DE_GOLD"."DIM_USER"(user_id, address, age, name, registered_at, valid_from, is_effective)
      select user_id, address, age, name, registered_at, current_timestamp(), TRUE
      from stream_users_extract_cast
      where metadata$action='INSERT' and metadata$isupdate=TRUE;
COMMIT;