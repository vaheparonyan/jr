select
    fp.event_day
    ,fp.event_time
    ,fp.session_id
    ,fp.user_scid
    ,fp.user_browser_id
    ,fp.page_path
    ,rk.utm_medium
    ,rk.utm_source
    ,rk.utm_campaign
    ,rk.utm_channel
    ,rk.utm_brand
    ,rk.utm_inventory
    ,rk.utm_strategy
    ,fp.referrer_url
    ,fp.page_type
    ,fp.page_channel
    ,fp.page_division
    ,fp.page_country
    ,fp.page_id
    ,fp.parent_page_id
    ,fp.browser
    ,fp.browser_version
    ,fp.os
    ,NULL as query_term
    ,fp.user_logged_in
    ,fp.bot_flag
    ,NULL as request_path_hash
    ,NULL as as_referrer_type
    ,rk.mktg_ref_key
    ,pc.mobile_or_not
    ,case when ed.widget_name is not null then 1 else 0 end as is_expired_deal
    ,sc.session_count as session_page_count
    ,sc.session_start_time_ux
    ,sc.session_end_time_ux
    ,pc.platform as os_agg
    ,pc.browser_type as browser_agg
    ,rq.referrer_query_string
    ,mktg_ref_key as mktg_ref_key_for_reporting 
    
from
    seo_cubes.c_first_pv_sessions fp
    left outer join seo_cubes.c_mktg_ref_key_data rk on (fp.user_browser_id = rk.user_browser_id and fp.session_id = rk.session_id)
    left outer join seo_cubes.c_page_and_traffic_classification pc on (pc.user_browser_id = fp.user_browser_id and pc.session_id = fp.session_id)
    left outer join seo_cubes.c_expired_deal_pages ed on (ed.page_id = fp.page_id)
    left outer join seo_cubes.c_session_counts sc on (sc.user_browser_id = fp.user_browser_id and sc.session_id = fp.session_id)
    left outer join seo_cubes.c_referrer_query_string rq on (rq.user_browser_id = fp.user_browser_id and rq.session_id = fp.session_id)
where
    fp.dt = '2014-10-01'
