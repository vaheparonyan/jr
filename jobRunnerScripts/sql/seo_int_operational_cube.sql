DELETE FROM seo_analytics.cube_int_bld_session_metrics_fcs where date_value = '@parameter_date';

SQL_BREAK;

REPLACE INTO seo_analytics.cube_int_bld_session_metrics_fcs

select
    dd.`day` 
    ,dd.is_holiday
    ,dw.week_start
    ,lower(sm.browser_type)
    ,case 
        when sm.browser_type in ('Android','Chrome Mobile','Mobile Safari') then 'mobile' 
        else 'non-mobile' 
     end 
    ,sm.mktg_ref_key
    ,sm.page_channel
    ,sm.page_type
    ,sm.platform
    ,sm.query_type
    ,sm.url_pattern
    ,ra.active_flag
    ,ra.class_type
    ,ra.rule
    ,ra.tier
    ,ra.traffic_sub_source
    ,ra.traffic_type
    ,ra.traffic_source
    ,CONCAT(ra.traffic_source,'-',ra.traffic_sub_source)
    ,COALESCE(dc.country_name, 'none')
    ,COALESCE(dc.sf_country_id, -99)
    ,coalesce(dc.economic_area, 'none')
    ,coalesce(dc.economic_region, 'none')
    ,coalesce(dc.currency_code, 'none')
    ,coalesce(dc.continent_abbr, 'none')
    ,coalesce(dc.country_iso_code_2, 'none')
    ,sm.average_session_length
    ,sm.bookings
    ,sm.bounce_sessions
    ,sm.new_subscriptions
    ,sm.order_sessions
    ,sm.page_views
    ,sm.page_views_on_orders
    ,sm.revenue
    ,sm.sessions
    ,sm.new_subscribers
    ,sm.transactions
    ,sm.visitors
    ,sm.new_subscription_requests

from
    seo_analytics.int_bld_session_metrics_fcs sm
    left outer join seo_analytics.ref_attr_class ra on (ra.ref_attr_class_key = sm.mktg_ref_key)
    left outer join seo_analytics.dim_day dd on (dd.day_key = sm.event_day_key)
    left outer join seo_analytics.dim_country dc on (dc.country_id = sm.page_country_key)
    left outer join seo_analytics.dim_week dw on (dd.week_key = dw.week_key)
where
    sm.event_day_key = '@parameter_dtkey'
;

