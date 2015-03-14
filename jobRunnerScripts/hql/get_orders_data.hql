SET mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

INSERT OVERWRITE TABLE ${db_name}.c_session_orders_tmp2 PARTITION (dt = '${date_value}')

select
    sc.event_day
    ,sc.session_id
    ,order_id
    ,tracking_cookie
    ,user_key
    ,case when substr(od.first_purchase_date, 1, 10) = '${date_value}' then 1 else 0 end 
    ,od.sale_amount
    ,od.cost_to_groupon
    ,od.cost_to_user 
from
    seo_cubes.c_session_orders_tmp od
    join seo_cubes.c_session_counts sc on (sc.event_day = COALESCE(od.orders_date_key,'none') and sc.user_browser_id = COALESCE(od.tracking_cookie,'none') and sc.dt = od.dt)
where
    sc.dt = '${date_value}'
    and od.dt = '${date_value}'
    and od.src_created_date_ux >= sc.session_start_time_ux
    and od.src_created_date_ux <= sc.session_end_time_ux

;

INSERT OVERWRITE TABLE ${db_name}.c_session_orders PARTITION (dt = '${date_value}')
select
    event_day
    ,session_id
    ,count(*)
    ,user_key
    ,first_purchasers
    ,sum(sale_amount)
    ,sum(sale_amount * (cost_to_groupon / cost_to_user))
    ,user_browser_id
from
    seo_cubes.c_session_orders_tmp2 
where
    dt = '${date_value}'
group by
        event_day
    ,session_id
    ,user_key
    ,first_purchasers
    ,user_browser_id 
;
