set mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;
SET mapred.task.timeout=1200000; 
INSERT OVERWRITE TABLE  ${db_name}.c_na_data_cube PARTITION (dt='${date_value}')

select
    dd.is_holiday 
    ,COALESCE(fp.browser_agg,'none')
    ,COALESCE(fp.is_mobile,'none') 
    ,COALESCE(fp.is_expired_deal,'none')
    ,COALESCE(fp.mktg_ref_key,'none') 
    ,COALESCE(fp.page_type ,'none')
    ,COALESCE(fp.page_channel,'none')
    ,COALESCE(fp.os_agg,'none')  
    ,'none1'  
    ,'none2' 
    ,COALESCE(tc.weekly_reporting_page_type,'none') 
    ,'none3'  
    ,COALESCE(tc.marketing_sub_channel,'none') 
    ,COALESCE(rf.traffic_sub_source,'none') 
    ,COALESCE(rf.traffic_type,'none') 
    ,COALESCE(rf.traffic_source,'none')
    ,COALESCE(CONCAT_WS('-',rf.traffic_source, rf.traffic_sub_source),'none')
    ,COUNT(DISTINCT(fp.user_browser_id))
    ,COUNT(*)
    ,COUNT(DISTINCT (CASE WHEN fp.session_page_count < 2 THEN fp.session_id ELSE null END))
    ,SUM(CASE WHEN fp.session_page_count < 100 THEN fp.session_page_count ELSE 0 END) 
    ,COUNT(DISTINCT (od.session_id))
    ,SUM(CASE WHEN od.session_id IS NOT NULL AND fp.session_page_count < 100 THEN session_page_count ELSE 0 END) 
    ,AVG(session_end_time_ux-session_start_time_ux) 
    ,SUM(od.transactions) 
    ,SUM(od.bookings) 
    ,SUM(od.cogs) 
    ,(SUM(od.bookings) - SUM(od.cogs)) 
    ,COUNT(DISTINCT(od.user_key)) 
    ,sum(od.first_purchasers) 
    ,count(distinct(case when ss.sub_subscription_history_key is not NULL then ss.sub_user_key else NULL END)) 
    ,count(distinct(ss.sup_user_key))
from
    seo_cubes.c_first_pv_data fp
    left outer join seo_cubes.dim_day dd on (dd.day_key = fp.event_day)
    left outer join seo_cubes.c_session_orders od on (od.session_id = COALESCE(fp.session_id,'none') and od.user_browser_id = COALESCE(fp.user_browser_id,'none') and od.dt = fp.dt)
    left outer join seo_cubes.c_subscribers_subscriptions ss on (ss.session_id = COALESCE(fp.session_id,'none') and ss.dt = fp.dt) 
    left outer join seo_cubes.c_page_and_traffic_classification tc on (tc.session_id = COALESCE(fp.session_id,'none') and tc.user_browser_id = COALESCE(fp.user_browser_id,'none') and tc.dt = fp.dt)
    left outer join seo_cubes.c_mktg_ref_key_attributes rf on (rf.session_id = COALESCE(fp.session_id,'none') and rf.user_browser_id = COALESCE(fp.user_browser_id,'none') and rf.dt = fp.dt)
where
    fp.dt = '${date_value}' 
group by
    dd.is_holiday
    ,COALESCE(fp.browser_agg,'none')
    ,COALESCE(fp.is_mobile,'none') 
    ,COALESCE(fp.is_expired_deal,'none')
    ,COALESCE(fp.mktg_ref_key,'none')
    ,COALESCE(fp.page_type ,'none')
    ,COALESCE(fp.page_channel,'none')
    ,COALESCE(fp.os_agg,'none')
    ,'none1'  
    ,'none2' 
    ,COALESCE(tc.weekly_reporting_page_type,'none')
    ,'none3'         
    ,COALESCE(tc.marketing_sub_channel,'none')
    ,COALESCE(rf.traffic_sub_source,'none')
    ,COALESCE(rf.traffic_type,'none')
    ,COALESCE(rf.traffic_source,'none')
    ,COALESCE(CONCAT_WS('-',rf.traffic_source, rf.traffic_sub_source),'none')
