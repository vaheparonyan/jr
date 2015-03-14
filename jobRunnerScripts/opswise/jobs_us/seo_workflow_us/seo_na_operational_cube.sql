DELETE FROM seo_analytics.rg_bld_session_metrics_fcs where date_value = '${date_value}';


REPLACE INTO seo_analytics.rg_bld_session_metrics_fcs
select
    dd.`day` 
    ,dd.is_holiday
    ,lower(sm.browser_type)
    ,case 
        when sm.browser_type in ('Android','Chrome Mobile','Mobile Safari') then 'mobile' 
        else 'non-mobile' 
     end 
    ,sm.is_expired_deal
    ,sm.mktg_ref_key
    ,sm.page_channel
    ,sm.page_type
    ,case
         when sm.page_type in ('browse/deals/index', 'local/city/category') then 'CityorCitycat'
         when sm.page_type in ('home/index','home/index-seo','homepage','homepage/index','local/home') then 'HP'
     else 'others'
     end 
    ,sm.platform
    ,sm.query_type
    ,pt.summary_cat_chan
    ,case
         when sm.page_type in ('homepage/index','home/index-seo','browse/deals/index','subscriptions/itier/local-zip') then '1.Homepage'
         when sm.page_type in ('home/index','local/city') then '2.City Page'
         when sm.page_type = 'local/city/category' then '3.City+Cat SERP'
         when sm.page_type = 'merchant/show' then '4./biz'
         when sm.page_channel = 'getaways'  then '6.Getaways'
         when sm.page_channel = 'goods' then '7.Goods'
         when (sm.page_type = 'deals/show' and sm.is_expired_deal =1 )then '5.Local Deal Exp'
         when (sm.page_type = 'deals/show' and sm.is_expired_deal =0 ) then '5.Local Deal Active'
         when (sm.page_type like '%coupons%' or sm.page_type like '%touch-coupons%') then '8.Coupons'
         when sm.page_type in ('stores/show_brand','stores/index') then '9.Brand'
     else '10.Others'
     end 
    ,case 
        when pt.summary_cat_chan like 'Local%' then 'Local'
        when pt.summary_cat_chan like 'Home%' then 'Home'
        when pt.summary_cat_chan like 'Goods%' then 'Goods'
        when pt.summary_cat_chan like 'Getaways%' then 'Getaways'
        when pt.summary_cat_chan like 'Occasions%' then 'Occasions'
        when pt.summary_cat_chan like 'Coupons%' then 'Coupons'
    else
        'Other'
    end 
    ,ra.traffic_sub_source
    ,ra.traffic_type
    ,ra.traffic_source
    ,CONCAT(ra.traffic_source,'-',ra.traffic_sub_source)
    ,sm.average_session_length
    ,sm.bookings
    ,sm.bounce_sessions
    ,sm.cogs
    ,sm.new_purchasers
    ,sm.new_subscriptions
    ,sm.order_sessions
    ,sm.page_views
    ,sm.page_views_on_orders
    ,sm.purchasers
    ,sm.revenue
    ,sm.sessions
    ,sm.subscribers
    ,sm.transactions
    ,sm.visitors

from
    seo_analytics.bld_session_metrics_fcs sm
    left outer join seo_analytics.dim_seo_page_type pt on (pt.seo_page_type_key = sm.seo_page_type_key)
    left outer join seo_analytics.ref_attr_class ra on (ra.ref_attr_class_key = sm.mktg_ref_key)
    left outer join seo_analytics.dim_day dd on (dd.day_key = sm.event_day_key)
where
    sm.event_day_key = '${date_key}'
;

