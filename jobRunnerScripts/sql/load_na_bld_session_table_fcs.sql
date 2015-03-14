DELETE FROM sandbox.as_bld_session_counts_fcs; 

SQL_BREAK;

INSERT INTO sandbox.as_bld_session_counts_fcs
SELECT event_day_key,
  sandbox.mp_sessionid_trim(session_id),
  COUNT(*) AS session_page_count,
  MIN(event_time) AS session_start_time,
  MAX(event_time) AS session_end_time
FROM user_groupondw_sec.fact_clickstream
WHERE event_date = '@parameter_date'
AND event_type_key = 2
AND TRANSLATE_CHK(session_id USING UNICODE_TO_LATIN) = 0
GROUP BY 1,2;

SQL_BREAK;

-- grab just the first page viewed in each session for this date.
-- fact_clickstream is partitioned by event_date, so use this rather than event_day_key
-- Exclude any sessions from googlebot, and any sessions where session_page_count > 1000
DELETE FROM sandbox.as_tmp_bld_session_1st_pv_fcs;

SQL_BREAK;

INSERT INTO sandbox.as_tmp_bld_session_1st_pv_fcs
SELECT  event_day_key,
  event_date,
  event_time,
  sandbox.mp_sessionid_trim(session_id) AS session_id_trim,
  user_scid,
  cookie_b,
  page_url,
  source_page_key,
  param_utm_medium_key,
  param_utm_source_key,
  param_utm_campaign_key,
  referrer_url,
  referring_domain_key,
  page_type_key,
  page_channel_key,
  page_division_key,
  page_country_key,
  page_view_id,
  parent_page_view_id,
  duab.browser_name AS browser_name,
  duab.browser_version AS browser_version,
  os_key,
  http_referring_query_term,
  user_logged_in,
  bot_ind
FROM user_groupondw_sec.fact_clickstream f
JOIN user_groupondw.dim_user_agent_browser duab 
ON f.browser_key = duab.browser_key
WHERE event_date = '@parameter_date'
AND event_type_key = 2
AND TRANSLATE_CHK(session_id USING UNICODE_TO_LATIN) = 0
AND browser_name <> 'googlebot'
AND session_id_trim NOT IN ( SELECT session_id_trim FROM sandbox.as_bld_session_counts_fcs WHERE session_page_count >= 1000 )
QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id_trim || f.event_day_key ORDER BY event_time ASC, referrer_url DESC, parent_page_view_id DESC) = 1  ;


SQL_BREAK;

-- now do all the joins necessary to get values like utm_medium, utm_source, etc
DELETE FROM sandbox.as_tmp_bld_sess_1st_pv_fcs_2; 

SQL_BREAK;

COLLECT SUMMARY STATISTICS ON sandbox.as_bld_session_counts_fcs;

SQL_BREAK;

COLLECT STATISTICS
COLUMN (OS_KEY),
COLUMN (PAGE_CHANNEL_KEY ,PAGE_DIVISION_KEY),
COLUMN (PAGE_CHANNEL_KEY ,OS_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY),
COLUMN (PARAM_UTM_SOURCE_KEY),
COLUMN (PARAM_UTM_CAMPAIGN_KEY),
COLUMN (REFERRING_DOMAIN_KEY),
COLUMN (PAGE_TYPE_KEY),
COLUMN (PAGE_CHANNEL_KEY),
COLUMN (PAGE_DIVISION_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY ,PARAM_UTM_SOURCE_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY ,PARAM_UTM_CAMPAIGN_KEY),
COLUMN (PARAM_UTM_CAMPAIGN_KEY ,PAGE_TYPE_KEY),
COLUMN (PARAM_UTM_SOURCE_KEY ,PAGE_TYPE_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY ,PAGE_TYPE_KEY),
COLUMN (REFERRING_DOMAIN_KEY ,PAGE_TYPE_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_SOURCE_KEY , PARAM_UTM_CAMPAIGN_KEY
,REFERRING_DOMAIN_KEY ,PAGE_TYPE_KEY , PAGE_DIVISION_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_SOURCE_KEY , PARAM_UTM_CAMPAIGN_KEY
,REFERRING_DOMAIN_KEY ,PAGE_TYPE_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_SOURCE_KEY , PARAM_UTM_CAMPAIGN_KEY
,REFERRING_DOMAIN_KEY ,PAGE_CHANNEL_KEY , PAGE_DIVISION_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_SOURCE_KEY , PARAM_UTM_CAMPAIGN_KEY
,REFERRING_DOMAIN_KEY ,PAGE_CHANNEL_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_SOURCE_KEY , PARAM_UTM_CAMPAIGN_KEY
,REFERRING_DOMAIN_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_SOURCE_KEY , PARAM_UTM_CAMPAIGN_KEY ,PAGE_TYPE_KEY
,PAGE_CHANNEL_KEY ,PAGE_DIVISION_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_SOURCE_KEY , PARAM_UTM_CAMPAIGN_KEY ,PAGE_TYPE_KEY
,PAGE_CHANNEL_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_SOURCE_KEY , PARAM_UTM_CAMPAIGN_KEY ,PAGE_TYPE_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_SOURCE_KEY , PARAM_UTM_CAMPAIGN_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_CAMPAIGN_KEY , REFERRING_DOMAIN_KEY ,PAGE_TYPE_KEY
,PAGE_CHANNEL_KEY ,PAGE_DIVISION_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_CAMPAIGN_KEY , REFERRING_DOMAIN_KEY ,PAGE_TYPE_KEY
,PAGE_CHANNEL_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_CAMPAIGN_KEY , REFERRING_DOMAIN_KEY ,PAGE_TYPE_KEY),
COLUMN (PARAM_UTM_MEDIUM_KEY
,PARAM_UTM_CAMPAIGN_KEY , REFERRING_DOMAIN_KEY),
COLUMN (PARAM_UTM_SOURCE_KEY
,PARAM_UTM_CAMPAIGN_KEY , REFERRING_DOMAIN_KEY ,PAGE_TYPE_KEY
,PAGE_CHANNEL_KEY ,PAGE_DIVISION_KEY),
COLUMN (PARAM_UTM_SOURCE_KEY
,PARAM_UTM_CAMPAIGN_KEY , REFERRING_DOMAIN_KEY ,PAGE_TYPE_KEY
,PAGE_CHANNEL_KEY),
COLUMN (PARAM_UTM_SOURCE_KEY
,PARAM_UTM_CAMPAIGN_KEY , REFERRING_DOMAIN_KEY ,PAGE_TYPE_KEY),
COLUMN (PARAM_UTM_SOURCE_KEY
,PARAM_UTM_CAMPAIGN_KEY , REFERRING_DOMAIN_KEY),
COLUMN (PARAM_UTM_SOURCE_KEY
,PARAM_UTM_CAMPAIGN_KEY) ON
sandbox.as_tmp_bld_session_1st_pv_fcs;

SQL_BREAK;

INSERT INTO sandbox.as_tmp_bld_sess_1st_pv_fcs_2
SELECT f.event_day_key_t AS event_day_key,
  f.event_date,
  f.event_time,
  f.session_id_trim,
  f.user_scid,
  f.cookie_b,
  dum.name_raw AS utm_medium,
  CASE WHEN translate_chk(dus.name_raw using unicode_to_latin) = 0 THEN dus.name_raw ELSE NULL END AS utm_source,
  CASE WHEN translate_chk(duc.name_raw using unicode_to_latin) = 0 THEN duc.name_raw ELSE NULL END AS utm_campaign,
  CASE WHEN translate_chk(duc.channel using unicode_to_latin) = 0 THEN duc.channel ELSE NULL END AS utm_campaign_channel,
  CASE WHEN translate_chk(duc.brand using unicode_to_latin) = 0 THEN duc.brand ELSE NULL END AS utm_campaign_brand,
  CASE WHEN translate_chk(duc.inventory using unicode_to_latin) = 0 THEN duc.inventory ELSE NULL END AS utm_campaign_inventory,
  CASE WHEN translate_chk(duc.strategy using unicode_to_latin) = 0 THEN duc.strategy ELSE NULL END AS utm_campaign_strategy,
  CASE WHEN translate_chk(f.referrer_url using unicode_to_latin) = 0 THEN f.referrer_url ELSE NULL END AS referrer_url,
  CASE WHEN translate_chk(drd.domain_name_raw using unicode_to_latin) = 0 THEN drd.domain_name_raw ELSE NULL END AS referrer_domain,
  f.page_type_key ,
  f.page_channel_key,
  f.page_division_key,
  f.page_country_key,
  f.page_view_id,
  f.parent_page_view_id,
  f.browser_name,
  f.browser_version,
  f.os_key,
  f.query_term,
  f.user_logged_in,      
  f.bot_ind
FROM sandbox.as_tmp_bld_session_1st_pv_fcs f
JOIN user_groupondw.dim_utm_medium dum 
ON f.param_utm_medium_key = dum.utm_medium_key 
JOIN user_groupondw.dim_utm_source dus 
ON f.param_utm_source_key = dus.utm_source_key 
JOIN user_groupondw.dim_utm_campaign duc
ON f.param_utm_campaign_key = duc.utm_campaign_key
JOIN user_groupondw.dim_referrer_domain drd
ON f.referring_domain_key = drd.referrer_domain_key;

SQL_BREAK;

 COLLECT STATISTICS
COLUMN (EVENT_DAY_KEY),
COLUMN (PAGE_TYPE_KEY),
COLUMN (PAGE_CHANNEL_KEY),
COLUMN (PAGE_DIVISION_KEY),
COLUMN (OS_KEY),
COLUMN (SESSION_ID),
COLUMN (PAGE_DIVISION_KEY ,OS_KEY),
COLUMN (PAGE_CHANNEL_KEY ,OS_KEY),
COLUMN (EVENT_DAY_KEY ,SESSION_ID),
COLUMN (EVENT_DAY_KEY ,SESSION_ID
,PAGE_CHANNEL_KEY),
COLUMN (EVENT_DAY_KEY ,SESSION_ID
,PAGE_TYPE_KEY),
COLUMN (EVENT_DAY_KEY ,SESSION_ID
,PAGE_TYPE_KEY , PAGE_CHANNEL_KEY ,PAGE_DIVISION_KEY ,OS_KEY),
COLUMN (EVENT_DAY_KEY ,SESSION_ID
,PAGE_TYPE_KEY , PAGE_CHANNEL_KEY ,PAGE_DIVISION_KEY),
COLUMN (EVENT_DAY_KEY ,SESSION_ID
,PAGE_TYPE_KEY , PAGE_CHANNEL_KEY),
COLUMN (EVENT_DAY_KEY ,SESSION_ID
,PAGE_TYPE_KEY , PAGE_DIVISION_KEY),
COLUMN (EVENT_DAY_KEY ,SESSION_ID
,PAGE_CHANNEL_KEY , PAGE_DIVISION_KEY) ON
sandbox.as_tmp_bld_sess_1st_pv_fcs_2;

SQL_BREAK;

-- and finally, insert into our table 
DELETE FROM sandbox.as_bld_session_1st_pv_fcs WHERE event_day_key = @parameter_dtkey;

SQL_BREAK;

INSERT INTO sandbox.as_bld_session_1st_pv_fcs
SELECT f.event_day_key,
  event_date,
  event_time,
  f.session_id,
  user_scid,
  cookie_b,
  NULL AS request_path,
  utm_medium,
  utm_source,
  utm_campaign,
  utm_campaign_channel,
  utm_campaign_brand,
  utm_campaign_inventory,
  utm_campaign_strategy,
  referrer_url,
  referrer_domain,
  dpt.page_type_name AS page_type,
  dpc.page_channel_name AS page_channel,
  dpd.page_division_name AS page_division,
  page_country_key,
  page_view_id,
  parent_page_view_id,
  browser_name,
  browser_version,
  duao.os_name AS os_name,
  query_term,
  user_logged_in,
  bot_ind,
  NULL AS request_path_hash,
  sandbox.as_referrer_type(referrer_domain, 
    utm_medium, 
    utm_campaign, 
    utm_source) 
    AS as_referrer_type,
  syslib.bh_traffic_source(utm_medium, 
    utm_campaign_channel, 
    utm_campaign_brand, 
    utm_campaign_inventory, 
    utm_campaign_strategy, 
    referrer_domain, 
    utm_source, 
    utm_campaign) 
    AS mktg_ref_key,
  sandbox.as_is_mobile(os_name, browser_name) AS is_mobile,
  0 AS is_expired_deal,
  sc.session_page_count,
  sc.session_start_time,
  sc.session_end_time,
  CASE WHEN os_name in ('mac','windows','ios','android') then os_name ELSE 'Other' END AS os_agg,
  CASE WHEN browser_name IN ('ie', 'chrome', 'safari', 'firefox', 'mobile safari', 'android', 'chrome mobile', 'googlebot') THEN browser_name ELSE 'Other' END AS browser_agg,
  NULL AS referrer_query_string,
  NULL AS mktg_ref_key_for_reporting
FROM  sandbox.as_tmp_bld_sess_1st_pv_fcs_2 f
JOIN user_groupondw.dim_page_type dpt 
ON f.page_type_key = dpt.page_type_key 
JOIN user_groupondw.dim_page_channel dpc
ON f.page_channel_key = dpc.page_channel_key
JOIN user_groupondw.dim_page_division dpd
ON f.page_division_key = dpd.page_division_key
JOIN user_groupondw.dim_user_agent_os duao
ON f.os_key = duao.os_key    
JOIN sandbox.as_bld_session_counts_fcs sc
ON f.event_day_key = sc.event_day_key 
AND f.session_id = sc.session_id_trim
WHERE f.event_day_key = @parameter_dtkey; 

SQL_BREAK;

-- populate request_path from sandbox.as_tmp_bld_session_1st_pv_fcs
-- I get out of spool space error if I include this in either of the above insert-selects
-- so just do it after the fact
UPDATE sandbox.as_bld_session_1st_pv_fcs
FROM sandbox.as_tmp_bld_session_1st_pv_fcs AS t
    SET request_path = CASE WHEN TRANSLATE_CHK(SPLIT_PART(SPLIT_PART(SUBSTR(SPLIT_PART(CAST(page_url AS VARCHAR(2000)),'://', 2), INDEX(SPLIT_PART(CAST(t.page_url AS VARCHAR(2000)), '://', 2), '/')), '?', 1), '#', 1) USING UNICODE_TO_LATIN) = 0 
    THEN SPLIT_PART(SPLIT_PART(SUBSTR(SPLIT_PART(CAST(page_url AS VARCHAR(2000)),'://', 2), INDEX(SPLIT_PART(CAST(t.page_url AS VARCHAR(2000)), '://', 2), '/')), '?', 1), '#', 1)
    ELSE '/' END 
WHERE session_id = t.session_id_trim
    AND event_day_key = t.event_day_key_t
    AND event_day_key = @parameter_dtkey;

SQL_BREAK;

-- clean it up some more: remove trailing / and whitespace
UPDATE sandbox.as_bld_session_1st_pv_fcs
   SET request_path = trim(CASE WHEN (request_path <> '/' AND SUBSTR(request_path, CHARACTER_LENGTH(request_path), 1) = '/') THEN SUBSTR(request_path, 0, CHARACTER_LENGTH(request_path)) ELSE request_path END)
 WHERE event_day_key = @parameter_dtkey;

SQL_BREAK;

UPDATE sandbox.as_bld_session_1st_pv_fcs 
SET request_path_hash = hash_md5(request_path)
WHERE event_day_key = @parameter_dtkey;

SQL_BREAK;

UPDATE sandbox.as_bld_session_1st_pv_fcs
SET referrer_query_string =  COALESCE(TRIM(BOTH FROM OTRANSLATE(LOWER(URI_PERCENT_DECODE( sandbox.as_search_query_raw(referrer_domain, referrer_url))), '+' || x'01' || x'02' || x'03' || x'04' || x'05' || x'06' || x'07' || x'08' || x'09' || x'0A' || x'0B' || x'0C' || x'0D' || x'0E' || x'0F' || x'10' || x'11' || x'12' || x'13' || x'14' || x'15' || x'16' || x'17' || x'18' || x'19' || x'1B' || x'1C' || x'1D' || x'1E' || x'1F', x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20')), '') 
WHERE event_day_key = @parameter_dtkey;

SQL_BREAK;

-- hack: mktg_ref_key does not distinguish 'Oranic - Email' and 'Organic - Groupon'
-- traffic, just lumps it all into 'Organic Referral'. We'd like to be able to distinguish them
-- so check to see if our referrer_type flagged this 
UPDATE sandbox.as_bld_session_1st_pv_fcs
   SET mktg_ref_key_for_reporting = CASE WHEN mktg_ref_key = 26 
     THEN CASE WHEN as_referrer_type = 'Organic - Email' THEN 10042 
       WHEN as_referrer_type = 'Organic - Groupon' THEN 10041
       ELSE 26 END
     ELSE mktg_ref_key END
WHERE event_day_key = @parameter_dtkey;

SQL_BREAK;

COLLECT STATISTICS COLUMN (EVENT_DAY_KEY),
COLUMN (PARTITION),
COLUMN (REQUEST_PATH_HASH),
COLUMN (PAGE_VIEW_ID),
COLUMN (SESSION_END_TIME),
COLUMN (COOKIE_B),
COLUMN (SESSION_START_TIME),
COLUMN (SESSION_ID),
COLUMN (REQUEST_PATH ,REQUEST_PATH_HASH),
COLUMN (EVENT_DAY_KEY ,SESSION_ID),
COLUMN (EVENT_DAY_KEY ,COOKIE_B),
COLUMN (EVENT_DAY_KEY ,COOKIE_B ,PAGE_TYPE
, PAGE_CHANNEL ,MKTG_REF_KEY ,IS_EXPIRED_DEAL ,OS_AGG ,BROWSER_AGG),
COLUMN (EVENT_DAY_KEY ,PAGE_TYPE ,PAGE_CHANNEL
, MKTG_REF_KEY ,IS_EXPIRED_DEAL ,OS_AGG ,BROWSER_AGG) ON
sandbox.as_bld_session_1st_pv_fcs; 

SQL_BREAK;

-- update this dim with any new requests
-- Hack: some entries in dim_division have more than one division_key for a 
-- given permalink. Take the one with the lowest division_key 
INSERT  INTO sandbox.as_dim_seo_request_path
SELECT RANDOM(1,100000),
       d.request_path_hash,
       d.request_path,
       d.division_key,
       sandbox.seo_page_type_id(d.request_path, d.division_key) AS seo_page_type_key,
       NULL,
       NULL
FROM       
( SELECT s.request_path_hash,
         s.request_path,
         MIN(div.division_key) AS division_key 
    FROM (
         SELECT st.request_path, st.request_path_hash
           FROM as_bld_session_1st_pv_fcs st
      LEFT JOIN sandbox.as_dim_seo_request_path dim ON st.request_path_hash = dim.request_path_hash
          WHERE st.event_day_key =  @parameter_dtkey
          AND dim.request_path IS NULL
         GROUP BY 1,2 ) s
  LEFT JOIN user_groupondw.dim_division div
       ON split_part(s.request_path, '/', 2) = div.permalink 
  GROUP BY 1,2     
    ) d ;

SQL_BREAK;

 -- after adding new requests, collect some stats
COLLECT STATISTICS COLUMN (REQUEST_PATH),
COLUMN (LOCAL_LAST_PATH),
COLUMN (LOCAL_CATEGORY_KEY),
COLUMN (REQUEST_PATH_HASH),
COLUMN (SEO_PAGE_TYPE_KEY) ON
sandbox.as_dim_seo_request_path;

SQL_BREAK;

COLLECT STATISTICS COLUMN (URL) ON
sandbox.seo_categories_with_names;

SQL_BREAK;

-- for any request that is like /local, find the last part of the path and
-- use this to find its category, if applicable. 
-- Ideally we would search from right to left in string to find the last delimiter,
-- or reverse the string and find the first delimiter, but there's no easy way to do  
-- either in Teradata. So do this hack: assume that we will never have a /local/* 
-- url that has more than 8 parts to the path. Currently the most we have is 
-- 4, like /local/city/neighbourhood/category 
DELETE FROM sandbox.as_tmp_local_path; 

SQL_BREAK;

INSERT INTO sandbox.as_tmp_local_path 
SELECT a.seo_request_path_key AS seo_rp_key,
  SUBSTRING(a.request_path FROM a.delimiter_7 + 1) AS last_path
FROM (
  SELECT seo_request_path_key, 
    request_path,
    POSITION('/' IN request_path) AS delimiter_1, 
    delimiter_1 + POSITION('/' IN SUBSTRING(request_path FROM delimiter_1 + 1)) AS delimiter_2,
    delimiter_2 + POSITION('/' IN SUBSTRING(request_path FROM delimiter_2 + 1)) AS delimiter_3,
    delimiter_3 + POSITION('/' IN SUBSTRING(request_path FROM delimiter_3 + 1)) AS delimiter_4,
    delimiter_4 + POSITION('/' IN SUBSTRING(request_path FROM delimiter_4 + 1)) AS delimiter_5,
    delimiter_5 + POSITION('/' IN SUBSTRING(request_path FROM delimiter_5 + 1)) AS delimiter_6,
    delimiter_6 + POSITION('/' IN SUBSTRING(request_path FROM delimiter_6 + 1)) AS delimiter_7
  FROM sandbox.as_dim_seo_request_path
  WHERE request_path LIKE '/local/%'
  AND local_last_path IS NULL 
) a;

SQL_BREAK;

UPDATE sandbox.as_dim_seo_request_path 
FROM sandbox.as_tmp_local_path t
 SET local_last_path = t.last_path 
WHERE request_path LIKE '/local/%'
 AND local_last_path IS NULL
 AND seo_request_path_key = t.seo_rp_key ;

SQL_BREAK;

UPDATE sandbox.as_dim_seo_request_path 
FROM sandbox.seo_categories_with_names c  
 SET local_category_key = c.seo_id 
WHERE request_path LIKE '/local/%' 
 AND local_category_key IS NULL 
 AND local_last_path = c.url; 

SQL_BREAK;

-- flag any /local requests that don't match a category  
UPDATE sandbox.as_dim_seo_request_path 
 SET local_category_key = '-1'
WHERE request_path LIKE '/local/%'   
 AND local_category_key IS NULL;

SQL_BREAK;

UPDATE sandbox.as_bld_session_1st_pv_fcs 
SET is_expired_deal = 1
WHERE page_view_id IN 
( SELECT page_id FROM user_groupondw.bld_widgets w
  WHERE widget_name IN ('DealUnavailableSoldOut','DealUnavailableExpired')
  AND log_date = '@parameter_date;'
  AND page_id IN 
    ( SELECT page_view_id FROM sandbox.as_bld_session_1st_pv_fcs WHERE event_day_key =  @parameter_dtkey ) 
)
AND event_day_key = @parameter_dtkey;

SQL_BREAK;

DELETE FROM sandbox.as_tmp_bld_session_orders;

SQL_BREAK;

INSERT INTO sandbox.as_tmp_bld_session_orders
SELECT s.event_day_key AS log_date_key,
  fo.order_id,
  s.session_id,
  MAX(clxns.user_key),
  MAX(CASE WHEN u.first_purchase_date BETWEEN d.day_start AND d.day_end THEN 1 ELSE 0 END) AS new_purchaser,
  SUM(clxns.sale_amount),
  MAX(od.cost_to_groupon),
  MAX(od.cost_to_user)
FROM user_groupondw.fact_orders fo
JOIN 
(
  SELECT event_day_key,
    session_id,
    cookie_b,
    session_start_time,
    session_end_time
  FROM sandbox.as_bld_session_1st_pv_fcs p
  WHERE event_day_key = @parameter_dtkey
) s
ON fo.order_date_key = s.event_day_key
AND fo.tracking_cookie = s.cookie_b
AND fo.src_created_date BETWEEN s.session_start_time AND s.session_end_time
JOIN user_groupondw.fact_collections clxns ON fo.order_id = clxns.order_id 
JOIN user_groupondw.dim_deal_option od ON clxns.deal_option_key = od.deal_option_key
JOIN user_groupondw.dim_user u ON clxns.user_key = u.user_key
JOIN user_groupondw.dim_day d ON s.event_day_key = d.day_key
WHERE fo.order_date_key = @parameter_dtkey
GROUP BY 1,2,3;

SQL_BREAK;

DELETE FROM sandbox.as_bld_session_orders_agg WHERE log_date_key = @parameter_dtkey;

SQL_BREAK;

INSERT INTO sandbox.as_bld_session_orders_agg 
SELECT log_date_key,
  session_id,
  count(*) AS transactions,
  max(user_key) AS user_key,
  max(new_purchaser) AS new_purchaser,
  sum(sale_amount) AS bookings,
  sum(sale_amount*(cost_to_groupon/cost_to_user)) AS cogs
FROM sandbox.as_tmp_bld_session_orders
WHERE log_date_key = @parameter_dtkey
GROUP BY 1,2 ;

SQL_BREAK;

DELETE FROM sandbox.as_bld_session_metrics_fcs WHERE event_day_key = @parameter_dtkey;

SQL_BREAK;

 INSERT INTO sandbox.as_bld_session_metrics_fcs
SELECT tot.event_day_key,
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
  mktg_ref_key_for_reporting AS mktg_ref_key,
  os_agg AS platform,
  browser_agg AS browser_type,
  marketing.f_seo_brand(referrer_query_string) AS query_type,
  is_expired_deal,
  page_channel,
  page_type,
  seo_page_type_key,
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
  JOIN sandbox.as_dim_seo_request_path dr ON p.request_path_hash = dr.request_path_hash
  LEFT JOIN sandbox.as_bld_session_orders_agg o 
  ON p.session_id = o.session_id AND p.event_day_key = o.log_date_key
  WHERE p.event_day_key =  @parameter_dtkey
  GROUP BY 1,2,3,4,5,6,7,8,9
) tot
LEFT JOIN (
  SELECT todays_entries.event_day_key,
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
    JOIN user_groupondw.dim_subscription_history ds 
    ON p.cookie_b = ds.tracking_cookie
    WHERE event_day_key =  @parameter_dtkey
    AND f_date_to_int(CAST(ds.src_created_date AS DATE)) =  @parameter_dtkey
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
      AND f_date_to_int(CAST(ds.src_created_date AS DATE)) =  @parameter_dtkey
      AND status = 'active'
      GROUP BY 1
    ) 
    QUALIFY ROW_NUMBER() OVER(PARTITION BY user_key ORDER BY subscription_history_key ASC, COALESCE(src_created_date, valid_date_start) ASC) = 1
  ) first_entries
  ON todays_entries.subscription_history_key = first_entries.subscription_history_key 
  GROUP BY 1,2,3,4,5,6,7,8,9       
) sub ON tot.event_day_key = sub.event_day_key
  AND tot.mktg_ref_key = sub.mktg_ref_key
  AND tot.platform = sub.platform
  AND tot.browser_type = sub.browser_type
  AND tot.query_type = sub.query_type
  AND tot.is_expired_deal = sub.is_expired_deal
  AND tot.page_channel = sub.page_channel
  AND tot.page_type = sub.page_type
  AND tot.seo_page_type_key = sub.seo_page_type_key;


SQL_BREAK;
 
DELETE FROM sandbox.as_bld_session_counts_fcs; 

SQL_BREAK;

DELETE FROM sandbox.as_tmp_bld_session_1st_pv_fcs;

SQL_BREAK;

DELETE FROM sandbox.as_tmp_bld_sess_1st_pv_fcs_2; 

SQL_BREAK;

DELETE FROM sandbox.as_tmp_local_path; 

SQL_BREAK;

DELETE FROM sandbox.as_tmp_bld_session_orders;

