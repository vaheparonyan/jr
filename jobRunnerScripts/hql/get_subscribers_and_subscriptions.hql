set mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;


INSERT OVERWRITE TABLE ${db_name}.c_first_sessions PARTITION (dt='${date_value}')
select
    user_browser_id
    ,session_id
    ,1
from
    seo_cubes.c_first_pv_sessions
where
    dt = '${date_value}' 
;


INSERT OVERWRITE TABLE ${db_name}.c_subscribers_subscriptions PARTITION (dt= '${date_value}')
select
    a.event_day
    ,a.session_id
    ,a.sup_subscription_history_key
    ,a.sup_user_key
    ,b.sub_subscription_history_key
    ,b.sub_user_key
from
(
select
    fp.event_day
    ,fp.session_id
    ,sp.subscription_history_key as sup_subscription_history_key
    ,sp.user_key as sup_user_key
from
    seo_cubes.c_first_pv_sessions fp     
    join seo_cubes.c_first_sessions fs on (fp.session_id = fs.session_id)
    join seo_cubes.c_subscriptions_data sp on (sp.tracking_cookie = fp.user_browser_id and sp.dt_src_created_date = fp.dt)
where
    sp.row_number = 1
    and fp.dt = '${date_value}'
    and sp.dt = '${date_value}'
) a

LEFT OUTER JOIN

(
select
    sc.user_key as sub_user_key
    ,sc.subscription_history_key as sub_subscription_history_key
from
    seo_cubes.c_subscribers_data sc 
where
    sc.row_number = 1
    and sc.dt = '${date_value}'
) b
on a.sup_subscription_history_key = b.sub_subscription_history_key
