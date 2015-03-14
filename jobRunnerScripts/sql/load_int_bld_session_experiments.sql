DELETE FROM sandbox.int_bld_session_expt_metrics WHERE event_day_key = @parameter_dtkey;


SQL_BREAK;

INSERT INTO sandbox.int_bld_session_expt_metrics
SELECT rev.event_day_key,
  rev.page_country_key,
  rev.mktg_ref_key,
  rev.platform,
  rev.browser_type,
  rev.query_type,
  rev.page_channel,
  rev.page_type,
  rev.url_pattern,
  rev.getaway_subclass,
  rev.experiment,
  rev.variant,
  rev.layer,  
  rev.visitors,
  rev.sessions,
  rev.bounce_sessions,
  rev.order_sessions,
  rev.page_views,
  rev.page_views_on_orders,
  rev.average_session_length,
  rev.transactions,
  rev.bookings,
  rev.revenue,
  s.new_subscribers,
  s.new_subscription_requests,
  s.new_subscriptions
FROM (
  SELECT f.event_day_key,
  f.page_country_key,
  mktg_ref_key_for_reporting AS mktg_ref_key,
  os_agg AS platform,
  browser_agg AS browser_type,
  CASE WHEN referrer_query_string IS NULL OR referrer_query_string = '' THEN 'unknown'
    WHEN LOWER(referrer_query_string) LIKE ANY
      ('%gropoun%',
      '%nasdaq:grpn%',
      '%group on%',
      '%gropon%',
      '%grupon.com%',
      '%grooupon%',
      '%groupn%',
      '%groopon%',
      '%groupoon%',
      '%gouponcom%',
      '%goupn%',
      '%groupon%',
      '%grou[on%',
      '%gropu on%',
      '%groupan%',
      '%goupons.com%',
      '%grouon%',
      '%www.grou%',
      '%grounpon%',
      '%groupns%',
      '%groipon%',
      '%gropon.com%',
      '%http://gr.pn%',
      '%grou[pn%',
      '%grupon%',
      '%groupoj%',
      '%groupin%',
      '%groopons%',
      '%grouppn%',
      '%roupon%',
      '%gruopons%',
      '%group-on %',
      '%group \+on%',
      '%groupcompon%',
      '%grou[pon%',
      '%goupon%',
      '%gropupon%',
      '%group coupon%',
      '%grouppone%',
      '%groupcoupon%',
      '%grouipon%',
      '%group on.com%',
      '%gtoupon%',
      '%croupon.com%',
      '%groupoin%',
      '%gourpon%',
      '%groupob%',
      '%grouopon%',
      '%goupons%',
      '%goupon.com%',
      '%groupion%',
      '%grouopn%',
      '%griupon%',
      '%gropuon%',
      '%gorupon%',
      '%croupon%',
      '%gropun%',
      '%groupo%',
      '%grpupon%',
      '%grou[%',
      '%groupomn%',
      '%gr.pn%',
      '%groupen%',
      '%group[on%',
      '%groupom%',
      '%groupn.com%',
      '%groupoun%',
      '%grupons%',
      '%grpn%',
      '%goupoun%',
      '%gruopon%',
      '%groiupon%',
      '%grou%',
      '%geoupon%',
      '%www.group%',
      '%grouppon%',
      '%groupun%',
      '%goupon%',
      '%www.grou%',
      '%gr.pn%',
      '%goupon.com%',
      '%grou[pon%',
      '%grpupon%',
      '%grouppone%',
      '%groiupon%',
      '%groupcompon%',
      '%group-on %',
      '%griupon%',
      '%grpupon%',
      '%grou[on%',
      '%gropuon%',
      '%gorupon%',
      '%grouon%',
      '%gr.pn%',
      '%gruopon%',
      '%group-on %',
      '%grouppn%',
      '%grouon %',
      '%gropuon%',
      '%grouppn%',
      '%gorupon%',
      '%gorupon%',
      '%gropun%',
      '%groupion%',
      '%grouppn%',
      '%goupn%',
      '%gouponcom%',
      '%groopons%',
      '%grouppn%',
      '%goupn%',
      '%grouopon%',
      '%groopons%',
      '%groopon%',
      '%grouppn%',
      '%groupan%')
    THEN 'brand'
    ELSE'nonbrand' END AS query_type,
  page_channel,
  page_type,
  url_pattern,
  CASE WHEN 
        f.page_country_key in (243,110,76,208,83) and
        f.request_path LIKE ANY ('%reisen%','%viaggi%','%voyages%','%viajes%','%travel%' ) THEN 'yes' else 'no' end AS getaway_subclass, 
  COALESCE(ex.experiment,'none') as experiment, 
  COALESCE(ex.variant,'none') as variant,
  COALESCE(ex.layer,'none')  as layer,
  COUNT(DISTINCT f.cookie_b) AS visitors,
  COUNT(*) AS sessions,
  COUNT(DISTINCT CASE WHEN f.session_page_count < 2 THEN f.session_id ELSE null END) AS bounce_sessions,
  COUNT(DISTINCT o.session_id) AS order_sessions,
  SUM(CASE WHEN session_page_count < 100 THEN session_page_count ELSE 0 END) AS page_views,
  SUM(CASE WHEN o.session_id IS NOT NULL AND session_page_count < 100 THEN session_page_count ELSE 0 END) AS page_views_on_orders,
  AVG(CAST(EXTRACT(MINUTE FROM (session_end_time - session_start_time MINUTE(4) TO SECOND)) * 60 +
    EXTRACT(SECOND FROM (session_end_time - session_start_time MINUTE(4) TO SECOND)) AS INTEGER)) AS average_session_length,
  SUM(o.transactions) AS transactions,
  SUM(o.gross_bookings) AS bookings,
  SUM(o.gross_revenue) AS revenue
FROM sandbox.as_int_bld_session_1st_pv_fcs f
LEFT JOIN sandbox.as_int_bld_session_orders_agg o
ON f.session_id = o.session_id
AND f.event_day_key = o.event_day_key
LEFT JOIN ( 
    select 
        experiment
        ,session_id
        ,log_date_key    
        ,log_date
        ,variant
        ,layer
    from
        dwh_mart_view.bld_experiments 
    WHERE 
        log_date = '@parameter_date'
    and experiment = 'gsm_local_category_responsive'
) ex on ex.session_id = f.session_id
            and ex.log_date_key = f.event_day_key

WHERE f.event_day_key = @parameter_dtkey
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) rev
LEFT JOIN (
  SELECT f.event_day_key,
    f.page_country_key,
    mktg_ref_key_for_reporting AS mktg_ref_key,
    os_agg AS platform,
    browser_agg AS browser_type,
    CASE WHEN referrer_query_string IS NULL OR referrer_query_string = '' THEN 'unknown'
      WHEN LOWER(referrer_query_string) LIKE ANY
        ('%gropoun%',
        '%nasdaq:grpn%',
        '%group on%',
        '%gropon%',
        '%grupon.com%',
        '%grooupon%',
        '%groupn%',
        '%groopon%',
        '%groupoon%',
        '%gouponcom%',
        '%goupn%',
        '%groupon%',
        '%grou[on%',
        '%gropu on%',
        '%groupan%',
        '%goupons.com%',
        '%grouon%',
        '%www.grou%',
        '%grounpon%',
        '%groupns%',
        '%groipon%',
        '%gropon.com%',
        '%http://gr.pn%',
        '%grou[pn%',
        '%grupon%',
        '%groupoj%',
        '%groupin%',
        '%groopons%',
        '%grouppn%',
        '%roupon%',
        '%gruopons%',
        '%group-on %',
        '%group \+on%',
        '%groupcompon%',
        '%grou[pon%',
        '%goupon%',
        '%gropupon%',
        '%group coupon%',
        '%grouppone%',
        '%groupcoupon%',
        '%grouipon%',
        '%group on.com%',
        '%gtoupon%',
        '%croupon.com%',
        '%groupoin%',
        '%gourpon%',
        '%groupob%',
        '%grouopon%',
        '%goupons%',
        '%goupon.com%',
        '%groupion%',
        '%grouopn%',
        '%griupon%',
        '%gropuon%',
        '%gorupon%',
        '%croupon%',
        '%gropun%',
        '%groupo%',
        '%grpupon%',
        '%grou[%',
        '%groupomn%',
        '%gr.pn%',
        '%groupen%',
        '%group[on%',
        '%groupom%',
        '%groupn.com%',
        '%groupoun%',
        '%grupons%',
        '%grpn%',
        '%goupoun%',
        '%gruopon%',
        '%groiupon%',
        '%grou%',
        '%geoupon%',
        '%www.group%',
        '%grouppon%',
        '%groupun%',
        '%goupon%',
        '%www.grou%',
        '%gr.pn%',
        '%goupon.com%',
        '%grou[pon%',
        '%grpupon%',
        '%grouppone%',
        '%groiupon%',
        '%groupcompon%',
        '%group-on %',
        '%griupon%',
        '%grpupon%',
        '%grou[on%',
        '%gropuon%',
        '%gorupon%',
        '%grouon%',
        '%gr.pn%',
        '%gruopon%',
        '%group-on %',
        '%grouppn%',
        '%grouon %',
        '%gropuon%',
        '%grouppn%',
        '%gorupon%',
        '%gorupon%',
        '%gropun%',
        '%groupion%',
        '%grouppn%',
        '%goupn%',
        '%gouponcom%',
        '%groopons%',
        '%grouppn%',
        '%goupn%',
        '%grouopon%',
        '%groopons%',
        '%groopon%',
        '%grouppn%',
        '%groupan%')
      THEN 'brand'
      ELSE'nonbrand' END AS query_type,
    page_channel,
    page_type,
    url_pattern,
    COALESCE(ex.experiment,'none') as experiment,
    COALESCE(ex.variant,'none') as variant, 
    COALESCE(ex.layer,'none') as layer,
    CASE WHEN
        f.page_country_key in (243,110,76,208,83) and 
        f.request_path LIKE ANY ('%reisen%','%viaggi%','%voyages%','%viajes%','%travel%' ) THEN 'yes' else 'no' end AS getaway_subclass,
    COUNT(DISTINCT new_sub_subscription_id) AS new_subscribers,
    COUNT(DISTINCT subscription_request_id) AS new_subscription_requests,
    COUNT(DISTINCT valid_subscription_id) AS new_subscriptions
  FROM sandbox.as_int_bld_session_1st_pv_fcs f
    LEFT JOIN (
    select
        experiment
        ,session_id
        ,log_date_key
        ,log_date
        ,variant
        ,layer
    from
        dwh_mart_view.bld_experiments
    WHERE
        log_date = '@parameter_date'
    and experiment = 'gsm_local_category_responsive'
    ) ex on ex.session_id = f.session_id
            and ex.log_date_key = f.event_day_key
  JOIN
  (
  SELECT a.event_date_key,
    s.subscription_id AS subscription_request_id,
    CASE WHEN s.legal_condition = 1 THEN s.subscription_id ELSE null END AS valid_subscription_id,
    newsubs.subscription_id AS new_sub_subscription_id,
    s.country_id,
    a.cookie_b
  FROM dwh_base_sec_view.subscriptions s
  LEFT JOIN dwh_mart_view.attr_subscriptions a
      ON CAST(s.subscription_id AS VARCHAR(50)) = a.subscription_id
      AND s.country_id = a.country_id
      AND a.attribution_type = 'first'
      AND CAST(s.created_at AS DATE) = '@parameter_date'
  LEFT JOIN (
    SELECT subscription_id,
      country_id,
      user_id
    FROM dwh_base_sec_view.subscriptions
    WHERE legal_condition = 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, country_id ORDER BY created_at) = 1
    ) newsubs
  ON s.subscription_id = newsubs.subscription_id
  AND s.country_id = newsubs.country_id
  AND s.user_id = newsubs.user_id
  WHERE a.event_date_key = @parameter_dtkey 
  AND a.attribution_type = 'first'
  ) subs
  ON f.event_day_key = subs.event_date_key
  AND f.cookie_b = subs.cookie_b
  AND f.page_country_key = subs.country_id
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
) s
ON rev.event_day_key = s.event_day_key
AND rev.page_country_key = s.page_country_key
AND rev.mktg_ref_key = s.mktg_ref_key
AND rev.platform = s.platform
AND rev.browser_type = s.browser_type
AND rev.query_type = s.query_type
AND rev.page_channel = s.page_channel
AND rev.page_type = s.page_type
AND rev.url_pattern = s.url_pattern
AND rev.getaway_subclass = s.getaway_subclass
AND rev.experiment = s.experiment
AND rev.variant = s.variant
AND rev.layer = s.layer    ;
