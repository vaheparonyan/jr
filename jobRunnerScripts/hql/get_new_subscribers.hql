SET mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

INSERT OVERWRITE TABLE ${db_name}.c_subscribers_data_temp1 PARTITION (dt = '${date_value}')

select
    b.user_key
 from
    seo_cubes.c_first_pv_sessions a
    join td_backup.dim_subscription_history b on (a.user_browser_id = b.tracking_cookie)
where
    a.dt = '${date_value}' 
    and substr(b.src_created_date,1,10) = '${date_value}' 
    and b.status = 'active'
group by
        b.user_key
;

INSERT OVERWRITE TABLE ${db_name}.c_subscribers_data_temp2 PARTITION (dt = '${date_value}')

select
    sh.user_key
    ,sh.tracking_cookie
    ,sh.src_created_date
    ,substr(sh.src_created_date, 1,10) as dt_src_created_date
    ,sh.valid_date_start
    ,sh.subscription_history_key
    ,unix_timestamp(sh.src_created_date) as ux_ts_cd
    ,unix_timestamp(sh.valid_date_start) as ux_ts_vs
    ,case when sh.src_created_date is NULL then unix_timestamp(sh.valid_date_start) else unix_timestamp(sh.src_created_date) END as sort_key_ts
from
    seo_cubes.c_subscribers_data_temp1 fd    
    join td_backup.dim_subscription_history sh  on (sh.user_key = fd.user_key)
where   
    fd.dt = '${date_value}'
;

INSERT OVERWRITE TABLE ${db_name}.c_subscribers_data PARTITION (dt = '${date_value}')

select
    a.*
    ,analytics_row_number(user_key)
from
(
select
     user_key 
    ,tracking_cookie
    ,src_created_date 
    ,dt_src_created_date 
    ,valid_start_date 
    ,subscription_history_key 
    ,src_created_date_ux 
    ,valid_date_start_ux 
    ,sort_key_ts
    from
        seo_cubes.c_subscribers_data_temp2
    where dt = '${date_value}'
    distribute by
        user_key
    sort by
       user_key ASC, subscription_history_key ASC, sort_key_ts ASC 
) a
    distribute by
        a.user_key
    sort by
       a.user_key ASC,a.subscription_history_key ASC, sort_key_ts ASC      
; 
