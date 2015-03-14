DELETE FROM sandbox.as_bot_view_test_metrics WHERE event_day_key = @parameter_dtkey; 

SQL_BREAK;

INSERT INTO sandbox.as_bot_view_test_metrics
  SELECT p.event_day_key,
    mktg_ref_key_for_reporting AS mktg_ref_key,
    os_agg AS platform,
    dr.local_category_key,
    page_type,
    p.request_path,
    COUNT(*) AS sessions,
    COUNT(DISTINCT CASE WHEN session_page_count < 2 THEN p.session_id ELSE null END) AS bounce_sessions,
    COUNT(DISTINCT o.session_id) AS order_sessions,
    SUM(transactions) AS transactions,
    SUM(bookings) AS bookings,
    (SUM(bookings) - SUM(cogs)) AS revenue
  FROM sandbox.as_bld_session_1st_pv_fcs p
  JOIN sandbox.as_dim_seo_request_path dr ON p.request_path_hash = dr.request_path_hash
  LEFT JOIN sandbox.as_bld_session_orders_agg o 
  ON p.session_id = o.session_id AND p.event_day_key = o.log_date_key
  WHERE p.event_day_key = @parameter_dtkey
  AND p.request_path LIKE '/local/%'
  AND p.request_path LIKE ANY ('%/arvada%',
    '%/aurora-co%',
    '%/centennial%',
    '%/denver%',
    '%/lakewood%',
    '%/thornton%',
    '%/westminster%',
    '%/berkley-co%',
    '%/brighton-co%',
    '%/castle-rock-co%',
    '%/columbine-co%',
    '%/commerce-city-co%',
    '%/englewood-co%',
    '%/federal-heights-co%',
    '%/golden-co%',
    '%/greenwood-village-co%',
    '%/highlands-ranch-co%',
    '%/ken-caryl-co%',
    '%/littleton-co%',
    '%/northglenn-co%',
    '%/parker-co%',
    '%/sherrelwood-co%',
    '%/welby-co%',
    '%/wheat-ridge-co%')
  GROUP BY 1,2,3,4,5,6;

