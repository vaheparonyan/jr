DELETE FROM sandbox.as_seo_url_metrics WHERE event_day_key = @parameter_dtkey;

SQL_BREAK;

INSERT INTO sandbox.as_seo_url_metrics
SELECT tot.event_day_key,
  tot.page_type,
  tot.request_path_hash,
  tot.sessions,
  tot.bounce_sessions,
  tot.order_sessions,
  tot.transactions,
  tot.bookings,
  tot.revenue,
  tot.purchasers,
  tot.new_purchasers,
  sub.new_subscribers,
  sub.new_subscriptions
FROM (
  SELECT p.event_day_key,
  page_type,
  request_path_hash,
  COUNT(*) AS sessions,
  COUNT(DISTINCT CASE WHEN session_page_count < 2 THEN p.session_id ELSE null END) AS bounce_sessions,
  COUNT(DISTINCT o.session_id) AS order_sessions,
  SUM(transactions) AS transactions,
  SUM(bookings) AS bookings,
  (SUM(bookings) - SUM(cogs)) AS revenue,
  COUNT(DISTINCT o.user_key) AS purchasers,
  COUNT(DISTINCT (CASE WHEN new_purchaser = 1 THEN o.user_key ELSE null END)) new_purchasers
  FROM sandbox.as_bld_session_1st_pv_fcs p
  JOIN user_groupondw.ref_attr_class r
  ON p.mktg_ref_key = r.ref_attr_class_key
  LEFT JOIN sandbox.as_bld_session_orders_agg o
  ON p.session_id = o.session_id AND p.event_day_key = o.log_date_key
  WHERE p.event_day_key = @parameter_dtkey
  AND page_type IN ('local/city/category', 'local/category', 'local/city', 'city_guide/articles', 'city_guide/vertical_home_page', 'browse/deals/index')
  AND r.traffic_source = 'SEO'
  GROUP BY 1,2,3
) tot
LEFT JOIN (
  SELECT todays_entries.event_day_key,
    page_type,
    request_path_hash,
    COUNT(DISTINCT CASE WHEN first_entries.subscription_history_key IS NOT NULL THEN first_entries.user_key ELSE NULL END) AS new_subscribers,
    COUNT(DISTINCT todays_entries.user_key) AS new_subscriptions
  FROM
  ( SELECT event_day_key,
      page_type,
      request_path_hash,
      ds.user_key,
      ds.subscription_history_key
    FROM sandbox.as_bld_session_1st_pv_fcs p
    JOIN user_groupondw.ref_attr_class r
    ON p.mktg_ref_key = r.ref_attr_class_key
    JOIN user_groupondw.dim_subscription_history ds
    ON p.cookie_b = ds.tracking_cookie
    WHERE event_day_key = @parameter_dtkey
    AND page_type IN ('local/city/category', 'local/category', 'local/city', 'city_guide/articles', 'city_guide/vertical_home_page', 'browse/deals/index')
    AND f_date_to_int(CAST(ds.src_created_date AS DATE)) = @parameter_dtkey
    AND status = 'active'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ds.user_key, CAST(ds.src_created_date AS DATE) ORDER BY subscription_history_key ASC, src_created_date ASC) = 1
  )todays_entries
  LEFT JOIN
  ( SELECT user_key,
      subscription_history_key
    FROM user_groupondw.dim_subscription_history
    WHERE user_key IN
    ( SELECT ds.user_key
      FROM sandbox.as_bld_session_1st_pv_fcs f
      JOIN user_groupondw.dim_subscription_history ds
      ON f.cookie_b = ds.tracking_cookie
      WHERE f.event_day_key = @parameter_dtkey
      AND f_date_to_int(CAST(ds.src_created_date AS DATE)) = @parameter_dtkey
      AND status = 'active'
      GROUP BY 1
    )
    QUALIFY ROW_NUMBER() OVER(PARTITION BY user_key ORDER BY subscription_history_key ASC, COALESCE(src_created_date, valid_date_start) ASC) = 1
  ) first_entries
  ON todays_entries.subscription_history_key = first_entries.subscription_history_key
  GROUP BY 1,2,3
) sub ON tot.event_day_key = sub.event_day_key
  AND tot.page_type = sub.page_type
  AND tot.request_path_hash = sub.request_path_hash;

SQL_BREAK;

-- maintain a monthly average over categories
DELETE FROM sandbox.as_seo_monthly_cat_metrics m
WHERE m.month_key = ( SELECT month_key FROM user_groupondw.dim_day WHERE day_key = @parameter_dtkey );

SQL_BREAK;

INSERT INTO sandbox.as_seo_monthly_cat_metrics
SELECT dd.month_key,
  c.seo_id,
  SUM(sessions) AS monthly_sessions,
  SUM(bounce_sessions) AS monthly_bounce_sessions,
  SUM(order_sessions) AS monthly_order_sessions,
  SUM(transactions) AS monthly_transactions,
  SUM(bookings) AS monthly_bookings,
  SUM(revenue) AS monthly_revenue,
  SUM(purchasers) AS monthly_purchasers,
  SUM(new_purchasers) AS monthly_new_purchasers,
  SUM(new_subscribers) AS monthly_new_subscribers,
  SUM(new_subscriptions) AS monthly_new_subscriptions
FROM sandbox.as_seo_url_metrics m
JOIN sandbox.as_dim_seo_request_path r
ON m.request_path_hash = r.request_path_hash
JOIN sandbox.seo_categories_with_names c
ON r.local_category_key = c.seo_id
JOIN user_groupondw.dim_day dd
ON m.event_day_key = dd.day_key
WHERE dd.month_key = ( SELECT month_key FROM user_groupondw.dim_day WHERE day_key = @parameter_dtkey )
AND page_type = 'local/city/category'
GROUP BY 1,2;

SQL_BREAK;

replace view sandbox.as_seo_monthly_url_metrics_v as
SELECT m.event_day_key/100 as month_key,
    page_type,
    r.request_path,
    COALESCE(SUM(m.sessions), 0) AS monthly_sessions,
    COALESCE(SUM(m.bounce_sessions), 0) AS monthly_bounce_sessions,
    COALESCE(SUM(m.order_sessions), 0) AS monthly_order_sessions,
    COALESCE(SUM(m.transactions), 0) AS monthly_transactions,
    COALESCE(SUM(m.bookings), 0) AS monthly_bookings,
    COALESCE(SUM(m.revenue), 0) AS monthly_revenue,
    COALESCE(SUM(m.purchasers), 0) AS monthly_purchasers,
    COALESCE(SUM(m.new_purchasers), 0) AS monthly_new_purchasers,
    COALESCE(SUM(m.new_subscribers), 0) AS monthly_new_subscribers,
    COALESCE(SUM(m.new_subscriptions), 0) AS monthly_new_subscriptions
 FROM sandbox.as_seo_url_metrics m
 JOIN sandbox.as_dim_seo_request_path r
    ON m.request_path_hash = r.request_path_hash
 GROUP BY 1,2,3;

SQL_BREAK;

replace view sandbox.kb_sitemapurls_monthly_view 
as 
select datekey/100 as month_key, url, pagetype, count(*) as monthly_count 
from sandbox.kb_sitemapurls 
group by 1,2,3;

SQL_BREAK;

replace view sandbox.kb_seo_view 
as select 
  kl.*,
  um.*,
  sm.month_key as sm_month_key,
  sm.avg_position as sm_avg_position,
  sm.avg_traffic as sm_avg_traffic,
  sm.avg_cpc as sm_avg_cpc,
  gp.locale,
  gp.search_volume,
  gp.competition,
  gp.average_cpc,
  COALESCE(stm.monthly_count, 0) as in_sitemap_monthly_count,
  crd.indexed as indexable,
  crd.isIndexed,
  di.month_key as di_month_key,
  di.avg_daily_deal_count,
  di.avg_daily_merchant_count
from sandbox.as_seo_monthly_url_metrics_v um 
join sandbox.seo_keywords_and_landings kl
on um.request_path = kl.local_url
left join sandbox.kb_searchmetrics_monthly sm 
on lower(kl.keyword) = sm.keyword and um.month_key = sm.month_key
left join sandbox.kb_google_potential gp 
on kl.keyword = gp.keyword_string 
left join sandbox.kb_seo_local_crawl_data crd 
on um.request_path = '/' || crd.url
left join sandbox.kb_seo_deal_inventory_monthly di 
on um.request_path = '/' || di.url and um.month_key = di.month_key
left join sandbox.seo_locations l
on um.request_path = '/local/' || l.location_url
left join sandbox.kb_sitemapurls_monthly_view stm
on stm.pagetype = 'pages' and um.request_path = '/' || stm.url and um.month_key = stm.month_key;

SQL_BREAK;

replace view sandbox.kb_seo_serps_monthly_view as
SELECT * FROM sandbox.kb_seo_view
    WHERE monthly_sessions > 0 OR (search_volume > 0 AND avg_daily_deal_count > 0);


