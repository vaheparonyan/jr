DELETE FROM sandbox.as_tmp_top_biz_pages; 

SQL_BREAK;


INSERT INTO sandbox.as_tmp_top_biz_pages
SELECT TOP 500 request_path_hash, count(*)
FROM sandbox.as_bld_session_1st_pv_fcs
WHERE event_day_key = @parameter_dtkey
AND request_path LIKE '/biz/%'
GROUP BY 1 ORDER BY 2 DESC;

SQL_BREAK;

DELETE FROM sandbox.as_bld_biz_metrics_fcs WHERE event_day_key = @parameter_dtkey;

SQL_BREAK;

INSERT INTO sandbox.as_bld_biz_metrics_fcs
SELECT tot.event_day_key,
  tot.mktg_ref_key,
  tot.platform,
  tot.browser_type,
  tot.query_type,
  tot.request_path,
  tot.visitors,
  tot.sessions,
  tot.bounce_sessions,
  tot.order_sessions,
  tot.page_views,
  tot.page_views_on_orders,
  tot.average_session_length,
  tot.transactions,
  tot.bookings,
  tot.cogs,
  tot.revenue,
  tot.purchasers,
  tot.new_purchasers,
  sub.new_subscribers,
  sub.new_subscriptions
FROM (
  SELECT p.event_day_key,
  mktg_ref_key_for_reporting AS mktg_ref_key,
  os_agg AS platform,
  browser_agg AS browser_type,
  marketing.f_seo_brand(referrer_query_string) AS query_type,
  request_path,
  COUNT(DISTINCT p.cookie_b) AS visitors,
  COUNT(*) AS sessions,
  COUNT(DISTINCT CASE WHEN session_page_count < 2 THEN p.session_id ELSE null END) AS bounce_sessions,
  COUNT(DISTINCT o.session_id) AS order_sessions,
  SUM(CASE WHEN session_page_count < 100 THEN session_page_count ELSE 0 END) AS page_views,
  SUM(CASE WHEN o.session_id IS NOT NULL AND session_page_count < 100 THEN session_page_count ELSE 0 END) AS page_views_on_orders,
  AVG(CAST(EXTRACT(MINUTE FROM (session_end_time - session_start_time MINUTE(4) TO SECOND)) * 60 +
    EXTRACT(SECOND FROM (session_end_time - session_start_time MINUTE(4) TO SECOND)) AS INTEGER)) AS average_session_length,
  SUM(transactions) AS transactions,
  SUM(bookings) AS bookings,
  SUM(cogs) AS cogs,
  (SUM(bookings) - SUM(cogs)) AS revenue,
  COUNT(DISTINCT o.user_key) AS purchasers,
  COUNT(DISTINCT (CASE WHEN new_purchaser = 1 THEN o.user_key ELSE null END)) new_purchasers
  FROM sandbox.as_bld_session_1st_pv_fcs p
  JOIN sandbox.as_tmp_top_biz_pages t 
  ON p.request_path_hash = t.request_path_hash
  LEFT JOIN sandbox.as_bld_session_orders_agg o 
  ON p.session_id = o.session_id AND p.event_day_key = o.log_date_key
  WHERE p.event_day_key = @parameter_dtkey
  AND request_path LIKE '/biz/%'
  GROUP BY 1,2,3,4,5,6
) tot
LEFT JOIN (
  SELECT todays_entries.event_day_key,
    mktg_ref_key,
    platform,
    browser_type,
    query_type,
    request_path,
    COUNT(DISTINCT CASE WHEN first_entries.subscription_history_key IS NOT NULL THEN first_entries.user_key ELSE NULL END) AS new_subscribers,
    COUNT(DISTINCT todays_entries.user_key) AS new_subscriptions
  FROM
  ( SELECT event_day_key,
      mktg_ref_key_for_reporting AS mktg_ref_key,
      os_agg AS platform,
      browser_agg AS browser_type,
      marketing.f_seo_brand(referrer_query_string) AS query_type,
      request_path,
      ds.user_key,
      ds.subscription_history_key
    FROM sandbox.as_bld_session_1st_pv_fcs p
    JOIN sandbox.as_tmp_top_biz_pages t 
    ON p.request_path_hash = t.request_path_hash
    JOIN user_groupondw.dim_subscription_history ds 
    ON p.cookie_b = ds.tracking_cookie
    WHERE event_day_key = @parameter_dtkey
    AND request_path LIKE '/biz/%'
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
  GROUP BY 1,2,3,4,5,6 
) sub ON tot.event_day_key = sub.event_day_key
  AND tot.mktg_ref_key = sub.mktg_ref_key
  AND tot.platform = sub.platform
  AND tot.browser_type = sub.browser_type
  AND tot.query_type = sub.query_type
  AND tot.request_path = sub.request_path;

INSERT INTO sandbox.as_bld_biz_all_views 
SELECT event_day_key,
  CASE WHEN TRANSLATE_CHK(SPLIT_PART(SPLIT_PART(SUBSTR(SPLIT_PART(CAST(page_url AS VARCHAR(2000)),'://', 2), INDEX(SPLIT_PART(CAST(page_url AS VARCHAR(2000)), '://', 2), '/')), '?', 1), '#', 1) USING UNICODE_TO_LATIN) = 0 
   THEN SPLIT_PART(SPLIT_PART(SUBSTR(SPLIT_PART(CAST(page_url AS VARCHAR(2000)),'://', 2), INDEX(SPLIT_PART(CAST(page_url AS VARCHAR(2000)), '://', 2), '/')), '?', 1), '#', 1)
   ELSE '/' END AS request_path ,
  count(*)
FROM user_groupondw_sec.fact_clickstream
WHERE event_date = '@parameter_date'
AND event_type_key = 2
AND page_url like '%/biz/%' 
GROUP BY 1,2 ;  

