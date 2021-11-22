BEGIN;
      merge into "MYDBT"."DE_GOLD"."DIM_USER" dim_user
            using (
              select *
              from stream_users_deduplicated
              where metadata$action='INSERT'
            ) s
            on s.USER_ID = dim_user.user_id
            -- we have updates for some users, so we set "is_effective" for these users
            when matched then
              update set
                  dim_user.is_effective = FALSE
            -- add new users to dim_user
            when not matched then
              insert (user_id, address, age, name, registered_at, valid_from, is_effective)
              values(s.user_id, s.address, s.age, s.name, s.registered_at, current_timestamp(), TRUE);

      -- add user updates to dim_user
      insert into "MYDBT"."DE_GOLD"."DIM_USER"(user_id, address, age, name, registered_at, valid_from, is_effective)
              select user_id, address, age, name, registered_at, current_timestamp(), TRUE
              from stream_users_deduplicated
              where metadata$action='INSERT' and metadata$isupdate=TRUE;
COMMIT;