-- DELETE FROM sandbox.as_int_bld_session_counts_fcs;
--
--
-- INSERT INTO sandbox.as_int_bld_session_counts_fcs
-- SELECT event_day_key,
--   session_id,
--   COUNT(*) AS session_page_count,
--   MIN(event_time) AS session_start_time,
--   MAX(event_time) AS session_end_time
-- FROM dwh_mart_view.fact_clickstream
-- WHERE event_date = '${date_value}'
-- AND event_type_key = 2
-- GROUP BY 1,2;
--
--
--
-- DELETE FROM sandbox.as_tmp_bld_session_1st_pv_fcs;
--
--
--
-- INSERT INTO sandbox.as_tmp_bld_session_1st_pv_fcs
-- SELECT f.event_day_key,
--   event_date,
--   event_time,
--   session_id,
--   user_scid,
--   cookie_b,
--   source_page_key,
--   param_utm_medium_key,
--   param_utm_source_key,
--   param_utm_campaign_key,
--   referrer_url,
--   referring_domain_key,
--   page_type_key,
--   page_channel_key,
--   page_division_key,
--   page_country_key,
--   page_view_id,
--   parent_page_view_id,
--   duab.browser_name AS browser_name,
--   duab.browser_version AS browser_version,
--   os_key,
--   http_referring_query_term,
--   page_url
-- FROM dwh_mart_view.fact_clickstream f
-- JOIN dwh_mart_view.dim_user_agent_browser duab
-- ON f.browser_key = duab.browser_key
-- WHERE event_date = '${date_value}'
-- AND event_type_key = 2
-- AND browser_name <> 'googlebot'
-- AND session_id NOT IN ( SELECT session_id FROM sandbox.as_int_bld_session_counts_fcs WHERE session_page_count >= 1000 )
-- GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id || f.event_day_key ORDER BY event_time ASC, referrer_url DESC, parent_page_view_id DESC) = 1 ;
--
--
--
-- DELETE FROM sandbox.as_tmp_bld_sess_1st_pv_fcs_2;
--
--
-- INSERT INTO sandbox.as_tmp_bld_sess_1st_pv_fcs_2
-- SELECT f.event_day_key_t AS event_day_key,
--   f.event_date,
--   f.event_time,
--   f.session_id,
--   f.user_scid,
--   f.cookie_b,
--   f.source_page_key,
--   CASE WHEN translate_chk(dum.name_raw using unicode_to_latin) = 0 THEN dum.name_raw ELSE NULL END AS utm_medium,
--   CASE WHEN translate_chk(dus.name_raw using unicode_to_latin) = 0 THEN dus.name_raw ELSE NULL END AS utm_source,
--   CASE WHEN translate_chk(duc.name_raw using unicode_to_latin) = 0 THEN duc.name_raw ELSE NULL END AS utm_campaign,
--   CASE WHEN translate_chk(duc.channel using unicode_to_latin) = 0 THEN duc.channel ELSE NULL END AS utm_campaign_channel,
--   CASE WHEN translate_chk(duc.brand using unicode_to_latin) = 0 THEN duc.brand ELSE NULL END AS utm_campaign_brand,
--   CASE WHEN translate_chk(duc.inventory using unicode_to_latin) = 0 THEN duc.inventory ELSE NULL END AS utm_campaign_inventory,
--   CASE WHEN translate_chk(duc.strategy using unicode_to_latin) = 0 THEN duc.strategy ELSE NULL END AS utm_campaign_strategy,
--   CASE WHEN translate_chk(f.referrer_url using unicode_to_latin) = 0 THEN f.referrer_url ELSE NULL END AS referrer_url,
--   CASE WHEN translate_chk(drd.domain_name_raw using unicode_to_latin) = 0 THEN drd.domain_name_raw ELSE NULL END AS referrer_domain,
--   f.page_type_key ,
--   f.page_channel_key,
--   f.page_division_key,
--   f.page_country_key,
--   f.page_view_id,
--   f.parent_page_view_id,
--   f.browser_name,
--   f.browser_version,
--   f.os_key,
--   f.query_term
-- FROM sandbox.as_tmp_bld_session_1st_pv_fcs f
-- JOIN dwh_mart_view.dim_utm_medium dum
-- ON f.param_utm_medium_key = dum.utm_medium_key
-- JOIN dwh_mart_view.dim_utm_source dus
-- ON f.param_utm_source_key = dus.utm_source_key
-- JOIN dwh_mart_view.dim_utm_campaign duc
-- ON f.param_utm_campaign_key = duc.utm_campaign_key
-- JOIN dwh_mart_view.dim_referrer_domain drd
-- ON f.referring_domain_key = drd.referrer_domain_key;
--
--
--
--
-- DELETE FROM sandbox.as_int_bld_session_1st_pv_fcs WHERE event_day_key = ${date_key};
--
--
-- INSERT INTO sandbox.as_int_bld_session_1st_pv_fcs
-- SELECT f.event_day_key,
--   event_date,
--   event_time,
--   f.session_id,
--   user_scid,
--   cookie_b,
--   CASE
--     when f.event_day_key <= 20140919 and TRANSLATE_CHK(dp.page_name USING UNICODE_TO_LATIN) = 0 THEN dp.page_name
--     ELSE '/' END AS request_path,
--   utm_medium,
--   utm_source,
--   utm_campaign,
--   utm_campaign_channel,
--   utm_campaign_brand,
--   utm_campaign_inventory,
--   utm_campaign_strategy,
--   referrer_url,
--   referrer_domain,
--   dpt.page_type_name AS page_type,
--   CASE
--       WHEN TRANSLATE_CHK(dpc.page_channel_name USING UNICODE_TO_LATIN) = 0
--       THEN dpc.page_channel_name
--       ELSE  CASE
--                 WHEN dpt.page_type_name = 'archive'
--                 THEN 'local'
--                 ELSE NULL
--             END
--   END AS page_channel,
--   CASE WHEN TRANSLATE_CHK(dpd.page_division_name USING UNICODE_TO_LATIN) = 0 THEN dpd.page_division_name ELSE NULL END AS page_division,
--   f.page_country_key,
--   page_view_id,
--   parent_page_view_id,
--   browser_name,
--   browser_version,
--   duao.os_name AS os_name,
--   query_term,
--   NULL AS request_path_hash,
--   NULL AS as_referrer_type,
--   syslib.bh_traffic_source(utm_medium,
--     utm_campaign_channel,
--     utm_campaign_brand,
--     utm_campaign_inventory,
--     utm_campaign_strategy,
--     referrer_domain,
--     utm_source,
--     utm_campaign)
--     AS mktg_ref_key,
--   CASE WHEN os_name IN ('Android', 'iOS', 'iPad', 'iPhone/iPod', 'Windows Phone') THEN 1
--     WHEN os_name LIKE 'BlackBerry%' THEN 1
--     WHEN browser_name = 'Amazon Silk' THEN 1
--     WHEN browser_name LIKE '%Mobile%' THEN 1
--     ELSE 0
--     END AS is_mobile,
--   0 AS is_expired_deal,
--   sc.session_page_count,
--   sc.session_start_time,
--   sc.session_end_time,
--   CASE WHEN os_name in ('mac','windows','ios','android') then os_name ELSE 'Other' END AS os_agg,
--   CASE WHEN browser_name IN ('ie', 'chrome', 'safari', 'firefox', 'mobile safari', 'android', 'chrome mobile', 'googlebot') THEN browser_name ELSE 'Other' END AS browser_agg,
--   NULL AS referrer_query_string,
--   NULL AS mktg_ref_key_for_reporting,
--   NULL AS url_pattern
-- FROM sandbox.as_tmp_bld_sess_1st_pv_fcs_2 f
-- JOIN dwh_mart_view.dim_page_type dpt
-- ON f.page_type_key = dpt.page_type_key
-- JOIN dwh_mart_view.dim_page_channel dpc
-- ON f.page_channel_key = dpc.page_channel_key
-- JOIN dwh_mart_view.dim_page_division dpd
-- ON f.page_division_key = dpd.page_division_key
-- JOIN dwh_mart_view.dim_user_agent_os duao
-- ON f.os_key = duao.os_key
-- LEFT OUTER JOIN dwh_mart_view.dim_page dp
-- ON f.source_page_key = dp.page_key
-- JOIN sandbox.as_int_bld_session_counts_fcs sc
-- ON f.event_day_key = sc.event_day_key
-- AND f.session_id = sc.session_id
-- WHERE f.event_day_key = ${date_key};
--
--
--
-- UPDATE sandbox.as_int_bld_session_1st_pv_fcs
-- FROM sandbox.as_tmp_bld_session_1st_pv_fcs AS t
--     SET request_path = CASE WHEN t.event_day_key_t > 20140919 and TRANSLATE_CHK(SPLIT_PART(SPLIT_PART(SUBSTR(SPLIT_PART(CAST(t.page_url AS VARCHAR(2000)),'://', 2), INDEX(SPLIT_PART(CAST(t.page_url AS VARCHAR(2000)), '://', 2), '/')), '?', 1), '#', 1) USING UNICODE_TO_LATIN) = 0
--     THEN SPLIT_PART(SPLIT_PART(SUBSTR(SPLIT_PART(CAST(t.page_url AS VARCHAR(2000)),'://', 2), INDEX(SPLIT_PART(CAST(t.page_url AS VARCHAR(2000)), '://', 2), '/')), '?', 1), '#', 1)
--     ELSE
--         CASE WHEN  t.event_day_key_t <=  20140919 then sandbox.as_int_bld_session_1st_pv_fcs.request_path END
--     END
-- WHERE sandbox.as_int_bld_session_1st_pv_fcs.session_id = t.session_id
--     AND sandbox.as_int_bld_session_1st_pv_fcs.event_day_key = t.event_day_key_t
--     AND sandbox.as_int_bld_session_1st_pv_fcs.event_day_key = ${date_key};
--
--
-- UPDATE sandbox.as_int_bld_session_1st_pv_fcs
--    SET request_path = trim(CASE WHEN (request_path <> '/' AND SUBSTR(request_path, CHARACTER_LENGTH(request_path), 1) = '/') THEN SUBSTR(request_path, 0, CHARACTER_LENGTH(request_path)) ELSE request_path END)
--  WHERE event_day_key = ${date_key};
--
--
-- UPDATE sandbox.as_int_bld_session_1st_pv_fcs
--    SET url_pattern = CASE
--     WHEN request_path = '/' THEN 'home'
--     WHEN page_type = 'archive' AND REGEXP_INSTR(request_path, '/(coupons|ofertas|gutscheine|bon-plan|kortingsbonnen|descontos|offre|tilbud|lahjakortti|prosfores|vouchers|offerte|kupong|oferta|descontos|erbjudande|firsat)$') > 0 THEN 'archive home'
--     WHEN page_type = 'archive' AND (page_division IS NOT NULL OR page_division <> 'unknown') AND REGEXP_INSTR(request_path, '/(coupons|ofertas|gutscheine|bon-plan|kortingsbonnen|descontos|offre|tilbud|lahjakortti|prosfores|vouchers|offerte|kupong|oferta|descontos|erbjudande|firsat)/.+') > 0 THEN 'archive city'
--     WHEN page_type = 'archive' AND (page_division IS NULL OR page_division = 'unknown') AND REGEXP_INSTR(request_path, '/(coupons|ofertas|gutscheine|bon-plan|kortingsbonnen|descontos|offre|tilbud|lahjakortti|prosfores|vouchers|offerte|kupong|oferta|descontos|erbjudande|firsat)/.+') > 0 THEN 'archive category'
--     WHEN page_type = 'archive' AND (page_division IS NOT NULL OR page_division <> 'unknown') AND REGEXP_INSTR(request_path, '/(coupons|ofertas|gutscheine|bon-plan|kortingsbonnen|descontos|offre|tilbud|lahjakortti|prosfores|vouchers|offerte|kupong|oferta|descontos|erbjudande|firsat)/[^/]+/.+') > 0 THEN 'archive city category'
--     WHEN REGEXP_INSTR(request_path, '/(deals|ofertas|oferty|offres)/events/[^/]+/\d+$') > 0 THEN '/deals/events/something/123'
--     WHEN REGEXP_INSTR(request_path, '/(deals|ofertas|oferty|offres)/[^/]+/[^/]+/\d+$') > 0 THEN '/deals/something/something/123'
--     WHEN REGEXP_INSTR(request_path, '/(deals|ofertas|oferty|offres)/[^/]*$') > 0 THEN '/deals/something'
--     WHEN REGEXP_INSTR(request_path, '/\d+$') > 0 THEN '/123'
--     WHEN REGEXP_INSTR(request_path, '/filter[^/]+/all-deals$') > 0 THEN '/filter*/all-deals'
--     WHEN REGEXP_INSTR(request_path, '/filter[^/]+/.+$') > 0 THEN '/filter*/something'
--     WHEN REGEXP_INSTR(request_path, '^/[^/]+$') > 0 THEN '/something'
--     ELSE 'other pattern'
--     END
-- WHERE event_day_key = ${date_key};


UPDATE sandbox.as_int_bld_session_1st_pv_fcs
   SET referrer_query_string = COALESCE(TRIM(BOTH FROM OTRANSLATE(LOWER(URI_PERCENT_DECODE(
    CASE WHEN (referrer_domain LIKE '%.google.%' AND referrer_domain NOT LIKE 'plus%.google.com' AND referrer_domain NOT LIKE 'mail.google.%') THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE '%search.yahoo.%' THEN nvp(referrer_url, 'p')
         WHEN referrer_domain LIKE '%.bing.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE '%search.aol.%' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE '%.aolsearch.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE 'www.aol.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE '%search.lycos.%' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE 'www.lycos.%' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE '%.ask.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE '%.about.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.daum.net' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.eniro.se' THEN nvp(referrer_url, 'search_word')
         WHEN referrer_domain LIKE '%search.naver.com' THEN nvp(referrer_url, 'query')
         WHEN referrer_domain = 'www.naver.com' THEN nvp(referrer_url, 'query')
         WHEN referrer_domain = 'cafe.naver.com' THEN nvp(referrer_url, 'query')
         WHEN referrer_domain = 'www.mamma.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE 'search%.voila.fr' THEN nvp(referrer_url, 'rdata')
         WHEN referrer_domain = 'ricerca.virgilio.it' THEN nvp(referrer_url, 'qs')
         WHEN referrer_domain = 'www.baidu.com' THEN nvp(referrer_url, 'wd')
         WHEN referrer_domain LIKE 'www.yandex.%' THEN nvp(referrer_url, 'text')
         WHEN referrer_domain LIKE 'yandex.%' THEN nvp(referrer_url, 'text')
         WHEN referrer_domain LIKE '%search.avg.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE '%.search-results.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE '%.delta-search.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE '%.claro-search.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.search.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'szukaj.wp.pl' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.kvasir.no' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'arama.mynet.com' THEN nvp(referrer_url, 'query')
         WHEN referrer_domain = 'nova.rambler.ru' THEN nvp(referrer_url, 'query')
         WHEN referrer_domain = 'www.mysearchresults.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'so.360.cn' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'start.funmoods.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'start.mysearchdial.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.holasearch.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.searchya.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'start.iminent.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.webcrawler.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.safesearch.net' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.similarsitesearch.com' THEN nvp(referrer_url, 'url')
         WHEN referrer_domain = 'msxml.excite.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'searchresults.verizon.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.searchmobileonline.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www1.dlinksearch.com' THEN nvp(referrer_url, 'url')
         WHEN referrer_domain = 'advancedsearch2.virginmedia.com' THEN nvp(referrer_url, 'searchquery')
         WHEN referrer_domain = 'addons.searchalot.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'lavasoft.blekko.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'searchassist.babylon.com' THEN nvp(referrer_url,' q')
         WHEN referrer_domain = 'www.41searchengines.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.searchamong.com' THEN nvp(referrer_url, 'query')
         WHEN referrer_domain = 'www.searchgol.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.searchinq.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.searchplusnetwork.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.zzsearch.net' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'www.so.com' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain = 'search.centurylink.com' THEN nvp(referrer_url, 'origurl')
         WHEN referrer_domain = 'search.sky.com' THEN nvp(referrer_url, 'term')
         WHEN referrer_domain = 'search.mywebsearch.com' THEN nvp(referrer_url, 'searchfor')
         WHEN referrer_domain = 'search.smartaddressbar.com' THEN nvp(referrer_url, 's')
         WHEN referrer_domain = 'search.bt.com' THEN nvp(referrer_url, 'p')
         WHEN referrer_domain = 'search.yam.com' THEN nvp(referrer_url, 'k')
         WHEN referrer_domain = 'search.bigpond.net.au' THEN nvp(referrer_url, 'searchquery')
         WHEN referrer_domain = 'search.charter.net' THEN nvp(referrer_url, 'querybox')
         WHEN referrer_domain = 'search.findwide.com' THEN nvp(referrer_url, 'k')
         WHEN referrer_domain = 'search.frontier.com' THEN nvp(referrer_url, 'origurl')
         WHEN referrer_domain = 'search.juno.com' THEN nvp(referrer_url, 'query')
         WHEN referrer_domain = 'search.maxonline.com.sg' THEN nvp(referrer_url, 'searchquery')
         WHEN referrer_domain = 'search.netzero.net' THEN nvp(referrer_url, 'query')
         WHEN referrer_domain = 'search.us.com' THEN nvp(referrer_url, 'k')
         WHEN referrer_domain LIKE 'search.%' THEN nvp(referrer_url, 'q')
         WHEN referrer_domain LIKE 'isearch.%' THEN nvp(referrer_url, 'q')
         ELSE NULL
    END
    )), '+' || x'01' || x'02' || x'03' || x'04' || x'05' || x'06' || x'07' || x'08' || x'09' || x'0A' || x'0B' || x'0C' || x'0D' || x'0E' || x'0F' || x'10' || x'11' || x'12' || x'13' || x'14' || x'15' || x'16' || x'17' || x'18' || x'19' || x'1B' || x'1C' || x'1D' || x'1E' || x'1F', x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20' || x'20')), '')
 WHERE event_day_key = ${date_key};


UPDATE sandbox.as_int_bld_session_1st_pv_fcs
   SET as_referrer_type =
    CASE WHEN utm_campaign_channel = 'SEA' AND (utm_medium IN ('cpc', 'cpm') OR utm_medium IS NULL) THEN
        CASE WHEN utm_campaign_inventory = 'pads' THEN 'SEM - Product Listings'
        WHEN utm_campaign_strategy = 'ttt' AND utm_campaign_brand = 'nbr' THEN 'SEM - RTC'
        WHEN utm_campaign_strategy = 'txn' THEN 'SEM - TXN'
        WHEN utm_campaign_brand = 'nbr' THEN 'SEM - Non-Brand'
        WHEN utm_campaign_brand = 'ybr' THEN 'SEM - Brand'
        ELSE 'Unknown UTM'
        END
    WHEN sandbox.as_search_referrer(referrer_domain) IS NOT NULL THEN 'Organic - Search'
    WHEN utm_campaign LIKE '%userreferral%' OR utm_campaign LIKE '%visitorreferral%' THEN
        CASE WHEN utm_medium = 'affiliate_link' THEN 'Affiliates - Other'
        WHEN utm_medium LIKE ANY ('raf%', 'affiliate%', 'link') THEN 'RAF'
        ELSE 'Unpaid User Referral'
        END
    WHEN utm_medium = 'email' THEN
        CASE WHEN utm_source LIKE 'newsletter%' THEN 'Email - G1'
        WHEN utm_source LIKE '%occasions%' THEN 'Email - Occasions'
        WHEN utm_source LIKE '%goods%' THEN 'Email - Goods'
        WHEN utm_source LIKE '%getaways%' THEN 'Email - Getaways'
        WHEN utm_source LIKE 'pc%' THEN 'Email - Personal Collections'
        ELSE 'Email - Other'
        END
    WHEN utm_campaign_channel IN ('CNT','EXP','DIS','SOM','EML','CPN','EXC','DPN','POR') AND (utm_medium = 'cpc' OR utm_medium IS NULL) THEN
        CASE WHEN utm_campaign_strategy IN ('NAQ', 'RAQ') THEN 'Display - NAQ'
        WHEN utm_campaign_strategy IN ('RRD','RRC','RRE','TTT','TTQ','RFS','ETTT') THEN 'Display - TTT'
        ELSE 'Unknown UTM' END
    WHEN utm_medium = 'afl' AND utm_source = 'rvs' THEN 'Affiliates - Aggregator'
    WHEN utm_medium = 'afl' AND utm_source = 'gpn' THEN 'Affiliates - GPN Platform'
    WHEN utm_medium = 'ptn' AND utm_source LIKE 'expedia%' THEN 'Affiliates - Expedia'
    WHEN utm_medium IN ('afl', 'ptn') THEN 'Affiliates - Other'
    WHEN utm_medium IN ('social', 'twitterfb', 'glivesocial') THEN 'Managed Social'
    WHEN utm_medium IN ('social_og_buy', 'app_center', 'facebook', 'twitter', 'pinterest') THEN 'Social Product'
    WHEN (utm_medium IS NULL or utm_medium = '#missing#')
      AND (utm_campaign IS NULL or utm_campaign = '#missing#')
      AND (utm_source IS NULL or utm_source IN ('#missing#', 'direct', 'general'))
      THEN CASE WHEN referrer_domain IS NULL THEN 'Organic - Direct'
        WHEN referrer_domain = 'NULL' THEN 'Organic - Direct'
        WHEN referrer_domain = '' THEN 'Organic - Direct'
        WHEN referrer_domain LIKE '%.groupon.%' THEN 'Organic - Groupon'
        WHEN referrer_domain LIKE '%gr.pn' THEN 'Organic - Groupon'
        WHEN referrer_domain LIKE '%.thepoint.com' THEN 'Organic - Groupon'
        WHEN referrer_domain LIKE '%.grouponworks.com' THEN 'Organic - Groupon'
        WHEN referrer_domain LIKE '%savored.com' THEN 'Organic - Groupon'
        WHEN referrer_domain LIKE '%.breadcrumbpos.com' THEN 'Organic - Groupon'
        WHEN referrer_domain LIKE '%.citydeal.com' THEN 'Organic - Groupon'
        WHEN referrer_domain LIKE '%.beeconomic.com.ph' THEN 'Organic - Groupon'
        WHEN referrer_domain = 'travel.yahoo.com' THEN 'Organic - SEO Link Partner'
        WHEN referrer_domain LIKE '%.uptake.com' THEN 'Organic - SEO Link Partner'
        WHEN referrer_domain LIKE '%.facebook.com' THEN 'Organic - Social'
        WHEN referrer_domain = 'fb.me' THEN 'Organic - Social'
        WHEN referrer_domain = 't.co' THEN 'Organic - Social'
        WHEN referrer_domain LIKE '%.twitter.com' THEN 'Organic - Social'
        WHEN referrer_domain LIKE '%.meetup.com' THEN 'Organic - Social'
        WHEN referrer_domain LIKE '%.tumblr.com' THEN 'Organic - Social'
        WHEN referrer_domain = 'lnkd.in' THEN 'Organic - Social'
        WHEN referrer_domain LIKE '%.linkedin.com' THEN 'Organic - Social'
        WHEN referrer_domain LIKE '%.pinterest.com' THEN 'Organic - Social'
        WHEN referrer_domain LIKE '%.reddit.com' THEN 'Organic - Social'
        WHEN referrer_domain LIKE '%.stumbleupon.com' THEN 'Organic - Social'
        WHEN referrer_domain LIKE 'plus%.google.com' THEN 'Organic - Social'
        WHEN referrer_domain = 'bit.ly' THEN 'Organic - Social'
        WHEN referrer_domain LIKE '%.digg.com' THEN 'Organic - Social'
        WHEN referrer_domain = 'su.pr' THEN 'Organic - Social'
        WHEN referrer_domain LIKE '%.meetup.com' THEN 'Organic - Social'
        WHEN referrer_domain LIKE '%.youtube.com' THEN 'Organic - Social'
        WHEN referrer_domain LIKE '%mail.%' THEN 'Organic - Email'
        WHEN referrer_domain LIKE '%m.gmx.%' THEN 'Organic - Email'
        WHEN referrer_domain LIKE 'www.gmx.%' THEN 'Organic - Email'
        WHEN referrer_domain = 'mm.web.de' THEN 'Organic - Email'
        WHEN referrer_domain = 'lavabit.com' THEN 'Organic - Email'
        WHEN referrer_domain = 'www.ya.ru' THEN 'Organic - Email'
        WHEN referrer_domain = 'wm.shaw.ca' THEN 'Organic - Email'
        WHEN (referrer_domain LIKE '%.google.%' AND referrer_domain NOT LIKE 'plus%.google.%' AND referrer_domain NOT LIKE 'mail.google.%' ) THEN 'Organic - Search'
        WHEN referrer_domain LIKE '%search.yahoo.%' THEN 'Organic - Search'
        WHEN referrer_domain LIKE '%.bing.com' THEN 'Organic - Search'
        WHEN referrer_domain LIKE '%search.aol.%' THEN 'Organic - Search'
        WHEN referrer_domain LIKE '%.aolsearch.com' THEN 'Organic - Search'
        WHEN referrer_domain LIKE 'www.aol.com' THEN 'Organic - Search'
        WHEN referrer_domain LIKE '%search.lycos.%' THEN 'Organic - Search'
        WHEN referrer_domain LIKE 'www.lycos.%' THEN 'Organic - Search'
        WHEN referrer_domain LIKE '%.ask.com' THEN 'Organic - Search'
        WHEN referrer_domain LIKE '%.about.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.daum.net' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.eniro.se' THEN 'Organic - Search'
        WHEN referrer_domain LIKE '%search.naver.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.naver.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'cafe.naver.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.mamma.com' THEN 'Organic - Search'
        WHEN referrer_domain LIKE 'search%.voila.fr' THEN 'Organic - Search'
        WHEN referrer_domain = 'ricerca.virgilio.it' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.baidu.com' THEN 'Organic - Search'
        WHEN referrer_domain LIKE 'www.yandex.%' THEN 'Organic - Search'
        WHEN referrer_domain LIKE 'yandex.%' THEN 'Organic - Search'
        WHEN referrer_domain LIKE '%search.avg.com' THEN 'Organic - Search'
        WHEN referrer_domain LIKE '%.search-results.com' THEN 'Organic - Search'
        WHEN referrer_domain LIKE '%.delta-search.com' THEN 'Organic - Search'
        WHEN referrer_domain LIKE '%.claro-search.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.search.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'szukaj.wp.pl' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.kvasir.no' THEN 'Organic - Search'
        WHEN referrer_domain = 'arama.mynet.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'nova.rambler.ru' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.mysearchresults.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'so.360.cn' THEN 'Organic - Search'
        WHEN referrer_domain = 'start.funmoods.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'start.mysearchdial.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.holasearch.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.searchya.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'start.iminent.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.webcrawler.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.safesearch.net' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.similarsitesearch.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'msxml.excite.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'searchresults.verizon.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.searchmobileonline.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www1.dlinksearch.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'advancedsearch2.virginmedia.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'addons.searchalot.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'lavasoft.blekko.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'searchassist.babylon.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.41searchengines.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.searchamong.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.searchgol.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.searchinq.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.searchplusnetwork.com' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.zzsearch.net' THEN 'Organic - Search'
        WHEN referrer_domain = 'www.so.com' THEN 'Organic - Search'
        WHEN referrer_domain LIKE 'search.%' THEN 'Organic - Search'
        WHEN referrer_domain LIKE 'isearch.%' THEN 'Organic - Search'
      ELSE 'Organic - Other'
      END
    ELSE 'Unknown UTM'
    END
WHERE event_day_key = ${date_key};



UPDATE sandbox.as_int_bld_session_1st_pv_fcs
   SET mktg_ref_key_for_reporting = CASE WHEN mktg_ref_key = 26
     THEN CASE WHEN as_referrer_type = 'Organic - Email' THEN 10042
       WHEN as_referrer_type = 'Organic - Groupon' THEN 10041
       ELSE 26 END
     ELSE mktg_ref_key END
WHERE event_day_key = ${date_key};


DELETE FROM sandbox.as_tmp_bld_session_orders;


INSERT INTO sandbox.as_tmp_bld_session_orders
SELECT event_day_key, session_id, page_country_key, CAST(order_id AS BIGINT)
FROM dwh_mart_view.fact_clickstream
WHERE event_date = '${date_value}'
AND order_id IS NOT NULL
AND UPPER(order_id)(CASESPECIFIC) = LOWER(order_id)(CASESPECIFIC)
GROUP BY 1,2,3,4 ;



INSERT INTO sandbox.as_int_bld_session_orders_agg
SELECT event_day_key,
  session_id,
  page_country_key,
  COUNT(DISTINCT b.billing_id) AS transactions,
  CAST(SUM(b.gross_revenue*b.cur_to_dollar_rate_at_creation) AS DECIMAL(9,2)) AS gross_revenue,
  CAST(SUM(b.gross_booking*b.cur_to_dollar_rate_at_creation) AS DECIMAL(9,2)) AS gross_booking
FROM sandbox.as_tmp_bld_session_orders t
JOIN dwh_mart_view.v_billings_ini_successful b
ON t.order_id = b.billing_id
AND t.page_country_key = b.country_id
GROUP BY 1,2,3;




DELETE FROM sandbox.as_int_bld_session_metrics_fcs WHERE event_day_key = ${date_key};


INSERT INTO sandbox.as_int_bld_session_metrics_fcs
SELECT rev.event_day_key,
  rev.page_country_key,
  rev.mktg_ref_key,
  rev.platform,
  rev.browser_type,
  rev.query_type,
  rev.page_channel,
  rev.page_type,
  rev.url_pattern,
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
WHERE f.event_day_key = ${date_key}
GROUP BY 1,2,3,4,5,6,7,8,9
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
    COUNT(DISTINCT new_sub_subscription_id) AS new_subscribers,
    COUNT(DISTINCT subscription_request_id) AS new_subscription_requests,
    COUNT(DISTINCT valid_subscription_id) AS new_subscriptions
  FROM sandbox.as_int_bld_session_1st_pv_fcs f
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
      AND CAST(s.created_at AS DATE) = '${date_value}'
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
  WHERE a.event_date_key = ${date_key}
  AND a.attribution_type = 'first'
  ) subs
  ON f.event_day_key = subs.event_date_key
  AND f.cookie_b = subs.cookie_b
  AND f.page_country_key = subs.country_id
  GROUP BY 1,2,3,4,5,6,7,8,9
) s
ON rev.event_day_key = s.event_day_key
AND rev.page_country_key = s.page_country_key
AND rev.mktg_ref_key = s.mktg_ref_key
AND rev.platform = s.platform
AND rev.browser_type = s.browser_type
AND rev.query_type = s.query_type
AND rev.page_channel = s.page_channel
AND rev.page_type = s.page_type
AND rev.url_pattern = s.url_pattern;


DELETE FROM sandbox.as_int_bld_session_counts_fcs;


DELETE FROM sandbox.as_tmp_bld_session_1st_pv_fcs;


DELETE FROM sandbox.as_tmp_bld_sess_1st_pv_fcs_2;


DELETE FROM sandbox.as_tmp_bld_session_orders;
