set mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

INSERT OVERWRITE TABLE ${db_name}.c_subscriptions_data PARTITION (dt = '${date_value}')

select
    a.user_key
    ,a.tracking_cookie
    ,a.src_created_date
    ,a.dt_src_created_date
    ,a.valid_date_start
    ,a.subscription_history_key
    ,a.ux_ts_cd
    ,a.ux_ts_vs
    ,analytics_row_number(a.user_key)
from
(
select
    user_key
    ,tracking_cookie
    ,src_created_date
    ,substr(src_created_date, 1,10) as dt_src_created_date
    ,valid_date_start
    ,subscription_history_key
    ,unix_timestamp(src_created_date) as ux_ts_cd
    ,unix_timestamp(valid_date_start) as ux_ts_vs
from    
   td_backup.dim_subscription_history
where
    substr(src_created_date, 1,10) = '${date_value}' 
    and status = 'active'
distribute by
    user_key, dt_src_created_date
sort by 
   user_key ASC,  dt_src_created_date ASC, subscription_history_key ASC, ux_ts_cd ASC 
) a 
distribute by
    a.user_key, a.dt_src_created_date    
sort by
    a.user_key ASC,  a.dt_src_created_date ASC, a.subscription_history_key ASC, a.ux_ts_cd ASC   
; 
