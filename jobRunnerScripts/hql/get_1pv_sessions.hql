set mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

INSERT OVERWRITE TABLE ${db_name}.c_first_pv_sessions_temp PARTITION (dt = '${date_value}')

SELECT
    a.event_day
    ,a.user_browser_id
    ,a.session_id
    ,a.event_time
    ,a.x_event_time
    ,a.parent_page_id
    ,a.page_id
    ,a.referrer_url
    ,a.user_scid
    ,a.page_url
    ,a.page_path
    ,a.utm_medium
    ,a.utm_source
    ,a.utm_campaign
    ,a.channel
    ,a.strategy
    ,a.inventory
    ,a.brand
    ,a.referrer_domain
    ,a.page_type
    ,a.page_channel
    ,a.page_division
    ,a.page_country
    ,a.browser
    ,a.browser_version
    ,a.os
    ,a.referrer_search_term
    ,a.user_logged_in
    ,a.widget_name
    ,a.bot_flag
FROM
(
select
    be.event_day
    ,COALESCE(be.user_browser_id,'none') as user_browser_id 
    ,CASE WHEN SUBSTR(COALESCE(be.session_id,'none'),1,32) not like '%-%' THEN SUBSTR(COALESCE(be.session_id,'none'),1,32) else substr(COALESCE(be.session_id,'none'),1,36) end as session_id 
    ,COALESCE(be.event_time,'none') as event_time
    ,unix_timestamp(be.event_time) as x_event_time 
    ,COALESCE(be.parent_page_id,'none') as parent_page_id
    ,COALESCE(be.page_id,'none') as page_id
    ,COALESCE(be.referrer_url,'none') as referrer_url
    ,COALESCE(be.user_scid,'none') as user_scid
    ,COALESCE(be.page_url,'none') as page_url
    ,COALESCE(be.page_path,'none') as page_path
    ,be.utm_medium 
    ,be.utm_source
    ,be.utm_campaign
    ,split(be.utm_campaign,"_")[2] as channel
    ,split(be.utm_campaign,"_")[5] as strategy
    ,split(be.utm_campaign,"_")[6] as inventory
    ,split(be.utm_campaign,"_")[9] as brand
    ,be.referrer_domain
    ,COALESCE(be.page_type,'none') as page_type
    ,COALESCE(be.page_channel,'none') as page_channel
    ,COALESCE(be.page_division,'none') as page_division
    ,COALESCE(be.page_country,'none') as page_country
    ,COALESCE(be.browser,'none') as browser
    ,COALESCE(be.browser_version,'none') as browser_version
    ,COALESCE(be.os,'none') as os
    ,COALESCE(be.referrer_search_term,'none') as referrer_search_term
    ,COALESCE(be.user_logged_in,'none') as user_logged_in
    ,COALESCE(be.widget_name,'none') as widget_name
    ,COALESCE(be.bot_flag,'none') as bot_flag
FROM
    default.bloodhound_events be
    join seo_cubes.c_session_counts sc on (sc.session_id = be.session_id)
WHERE 
    be.event = 'pageview'
    and be.dt = '${date_value}' 
    and be.browser != 'googlebot'
    and sc.session_count < 1000
    and be.page_country in ('US','CA')
) a
distribute by
    a.session_id
sort by
    (a.session_id ASC, a.x_event_time ASC, a.parent_page_id DESC, a.referrer_url DESC)
;

INSERT OVERWRITE TABLE ${db_name}.c_first_pv_sessions PARTITION (dt = '${date_value}')

select
    *
from
(
select
    event_day
    ,user_browser_id
    ,session_id
    ,event_time
    ,x_event_time
    ,analytics_row_number(session_id) as row_number
    ,parent_page_id
    ,page_id
    ,referrer_url
    ,user_scid
    ,page_url
    ,page_path
    ,utm_medium
    ,utm_source
    ,utm_campaign
    ,channel
    ,strategy
    ,inventory
    ,brand
    ,referrer_domain
    ,page_type
    ,page_channel
    ,page_division
    ,page_country
    ,browser
    ,browser_version
    ,os
    ,referrer_search_term
    ,user_logged_in
    ,widget_name
    ,bot_flag
from
    seo_cubes.c_first_pv_sessions_temp
where
    dt = '${date_value}'
DISTRIBUTE BY
    session_id
SORT BY
    (session_id ASC, x_event_time ASC, parent_page_id DESC, referrer_url DESC)
) a
where
    a.row_number = 1
;
