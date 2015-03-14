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
      AND page_type IN ('local/city/category', 'local/city', 'city_guide/articles', 'city_guide/vertical_home_page', 'browse/deals/index') 
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
        AND page_type IN ('local/city/category', 'local/city', 'city_guide/articles', 'city_guide/vertical_home_page', 'browse/deals/index') 
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
