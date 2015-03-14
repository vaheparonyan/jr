 --create a roll-up from bld_experiments which has one row per session_id per experiment
DELETE FROM sandbox.as_bld_exp_of_interest;

SQL_BREAK;

INSERT INTO sandbox.as_bld_exp_of_interest
SELECT log_date_key, experiment, variant, session_id
FROM user_groupondw.bld_experiments
WHERE experiment IN (SELECT experiment_name FROM sandbox.seo_ab_tests_of_interest)
AND log_date_key = @parameter_dtkey
GROUP BY 1,2,3,4;

SQL_BREAK;

DELETE FROM sandbox.as_bld_session_exp_metrics_fcs WHERE event_day_key = @parameter_dtkey;

SQL_BREAK;

INSERT INTO sandbox.as_bld_session_exp_metrics_fcs
SELECT tot.event_day_key,
  tot.experiment,
  tot.variant,
  tot.mktg_ref_key,
  tot.platform,
  tot.browser_type,
  tot.query_type,
  tot.is_expired_deal,
  tot.page_channel,
  tot.page_type,
  tot.seo_page_type_key,
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
  e.experiment,
  e.variant,
  p.mktg_ref_key_for_reporting AS mktg_ref_key,
  p.os_agg AS platform,
  p.browser_agg AS browser_type,
  marketing.f_seo_brand(p.referrer_query_string) AS query_type,
  p.is_expired_deal,
  p.page_channel,
  p.page_type,
  dr.seo_page_type_key,
  COUNT(DISTINCT p.cookie_b) AS visitors,
  COUNT(*) AS sessions,
  COUNT(DISTINCT CASE WHEN p.session_page_count < 2 THEN p.session_id ELSE null END) AS bounce_sessions,
  COUNT(DISTINCT o.session_id) AS order_sessions,
  SUM(CASE WHEN p.session_page_count < 100 THEN p.session_page_count ELSE 0 END) AS page_views,
  SUM(CASE WHEN o.session_id IS NOT NULL AND p.session_page_count < 100 THEN p.session_page_count ELSE 0 END) AS page_views_on_orders,
  AVG(CAST(EXTRACT(MINUTE FROM (p.session_end_time - p.session_start_time MINUTE(4) TO SECOND)) * 60 +
    EXTRACT(SECOND FROM (p.session_end_time - p.session_start_time MINUTE(4) TO SECOND)) AS INTEGER)) AS average_session_length,
  SUM(o.transactions) AS transactions,
  SUM(o.bookings) AS bookings,
  SUM(o.cogs) AS cogs,
  (SUM(o.bookings) - SUM(o.cogs)) AS revenue,
  COUNT(DISTINCT o.user_key) AS purchasers,
  COUNT(DISTINCT (CASE WHEN o.new_purchaser = 1 THEN o.user_key ELSE null END)) new_purchasers
  FROM sandbox.as_bld_session_1st_pv_fcs p
  JOIN sandbox.as_dim_seo_request_path dr 
  ON p.request_path_hash = dr.request_path_hash
  JOIN sandbox.as_bld_exp_of_interest e
  ON p.session_id = e.session_id AND p.event_day_key = e.log_date_key
  LEFT JOIN sandbox.as_bld_session_orders_agg o 
  ON p.session_id = o.session_id AND p.event_day_key = o.log_date_key
  WHERE p.event_day_key = @parameter_dtkey
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) tot
LEFT JOIN (
   SELECT todays_entries.event_day_key,
    experiment,
    variant,
    mktg_ref_key,
    platform,
    browser_type,
    query_type,
    is_expired_deal, 
    page_channel,
    page_type,   
    seo_page_type_key,         
    COUNT(DISTINCT CASE WHEN first_entries.subscription_history_key IS NOT NULL THEN first_entries.user_key ELSE NULL END) AS new_subscribers,
    COUNT(DISTINCT todays_entries.user_key) AS new_subscriptions
  FROM
  ( SELECT event_day_key,
      experiment,
      variant,
      mktg_ref_key_for_reporting AS mktg_ref_key,
      os_agg AS platform,
          browser_agg AS browser_type,
          marketing.f_seo_brand(referrer_query_string) AS query_type,
          is_expired_deal,
          page_channel,
          page_type, 
          seo_page_type_key,
          ds.user_key,
          ds.subscription_history_key
        FROM sandbox.as_bld_session_1st_pv_fcs p
        JOIN sandbox.as_dim_seo_request_path dr 
        ON p.request_path_hash = dr.request_path_hash
        JOIN sandbox.as_bld_exp_of_interest e 
        ON p.session_id = e.session_id AND p.event_day_key = e.log_date_key
        JOIN user_groupondw.dim_subscription_history ds 
        ON p.cookie_b = ds.tracking_cookie
        WHERE event_day_key = @parameter_dtkey
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
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11   
    ) sub ON tot.event_day_key = sub.event_day_key
      AND tot.mktg_ref_key = sub.mktg_ref_key
      AND tot.experiment = sub.experiment
      AND tot.variant = sub.variant
      AND tot.platform = sub.platform
      AND tot.browser_type = sub.browser_type
      AND tot.query_type = sub.query_type
      AND tot.is_expired_deal = sub.is_expired_deal
      AND tot.page_channel = sub.page_channel
      AND tot.page_type = sub.page_type
      AND tot.seo_page_type_key = sub.seo_page_type_key;


