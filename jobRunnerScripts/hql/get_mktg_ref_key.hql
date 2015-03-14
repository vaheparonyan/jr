SET mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

INSERT OVERWRITE TABLE ${db_name}.c_mktg_ref_key_data PARTITION (dt = '${date_value}')


select
    fp.user_browser_id
    ,fp.session_id 
    ,fp.utm_medium
    ,fp.channel 
    ,fp.brand 
    ,fp.inventory 
    ,fp.strategy 
    ,fp.referrer_domain 
    ,fp.utm_source 
    ,fp.utm_campaign 
    , CASE
        WHEN LOWER(fp.utm_medium) IN ('cpc', 'cpm') AND LOWER(fp.channel) = 'sea'
             THEN
               CASE
                 WHEN LOWER(fp.strategy) = 'lb'
                 THEN 45
                 WHEN LOWER(fp.brand) = 'ybr'
                 THEN 5
                 WHEN LOWER(fp.brand) = 'nbr'
                 THEN
                 CASE
                   WHEN LOWER(fp.strategy) = 'naq'
                   THEN 42
                   WHEN LOWER(fp.inventory) = 'pads'
                   THEN 8
                   ELSE 43
                 END
               ELSE 44
               END
             WHEN (
                       LOWER(fp.referrer_domain) LIKE '%.google.%'
                       AND LOWER(fp.referrer_domain) NOT LIKE 'plus%.google.%'
                       AND LOWER(fp.referrer_domain) NOT LIKE 'mail.google.%'
                  )
                  OR LOWER(fp.referrer_domain) LIKE '%search.yahoo.%'
                  OR LOWER(fp.referrer_domain) LIKE '%.bing.com'
                  OR LOWER(fp.referrer_domain) LIKE '%search.aol.%'
                  OR LOWER(fp.referrer_domain) LIKE '%.aolsearch.com'
                  OR LOWER(fp.referrer_domain) LIKE '%search.lycos.%'
                  OR LOWER(fp.referrer_domain) LIKE 'www.lycos.%'
                  OR LOWER(fp.referrer_domain) LIKE '%.ask.com'
                  OR LOWER(fp.referrer_domain) LIKE '%.about.com'
                  OR LOWER(fp.referrer_domain) LIKE '%search.naver.com'
                  OR LOWER(fp.referrer_domain) LIKE 'search%.voila.fr'
                  OR LOWER(fp.referrer_domain) LIKE 'www.yandex.%'
                  OR LOWER(fp.referrer_domain) LIKE 'yandex.%'
                  OR LOWER(fp.referrer_domain) LIKE '%search.avg.com'
                  OR LOWER(fp.referrer_domain) LIKE '%.search-results.com'
                  OR LOWER(fp.referrer_domain) LIKE '%.delta-search.com'
                  OR LOWER(fp.referrer_domain) LIKE '%.claro-search.com'
                  OR (LOWER(fp.referrer_domain) LIKE 'search.%' AND (fp.utm_campaign is NULL OR TRIM(fp.utm_campaign) = '') AND (fp.utm_medium is NULL OR TRIM(fp.utm_medium) = '') )
                  OR LOWER(fp.referrer_domain) LIKE 'isearch.%'
                  OR fp.referrer_domain IN ('www.aol.com', 'www.daum.net', 'www.eniro.se', 'www.naver.com', 'cafe.naver.com', 'www.mamma.com', 'ricerca.virgilio.it', 'www.baidu.com', 'www.search.com', 'szukaj.wp.pl', 'www.kvasir.no', 'arama.mynet.com', 'nova.rambler.ru', 'www.mysearchresults.com', 'so.360.cn', 'start.funmoods.com', 'start.mysearchdial.com', 'www.holasearch.com', 'www.searchya.com', 'start.iminent.com', 'www.webcrawler.com', 'www.safesearch.net', 'www.similarsitesearch.com', 'msxml.excite.com', 'searchresults.verizon.com', 'www.searchmobileonline.com', 'www1.dlinksearch.com', 'advancedsearch2.virginmedia.com', 'addons.searchalot.com', 'lavasoft.blekko.com', 'searchassist.babylon.com', 'www.41searchengines.com', 'www.searchamong.com', 'www.searchgol.com', 'www.searchinq.com', 'www.searchplusnetwork.com', 'www.zzsearch.net', 'www.so.com')
                  THEN 13
             WHEN LOWER(fp.utm_medium) in ('afl','aff','ptn','aff_10', 'aff_3286', 'aff_225', 'aff_14', 'aff_255', 'aff_2001')
             THEN
                 CASE
                     WHEN LOWER(fp.channel) ='ms'
                     THEN 59
                     WHEN LOWER(fp.channel) = 'ss'
                     THEN 60
                     WHEN LOWER(fp.channel) = 'cs'
                     THEN 61
                     WHEN LOWER(fp.channel) = 'cts'
                     THEN 62
                     WHEN LOWER(fp.channel) = 'coms'
                     THEN 63
                     WHEN LOWER(fp.channel) = 'di'
                     THEN 64
                     WHEN LOWER(fp.channel) = 'ad'
                     THEN 65
                     WHEN LOWER(fp.channel) = 'es'
                     THEN 66
                     WHEN LOWER(fp.channel) = 'vs'
                     THEN 67
                     WHEN LOWER(fp.utm_source) in ('rvs','omg','znx','awn')
                     THEN 1
                     WHEN LOWER(fp.utm_source) = 'gpn'
                     THEN 2
                     WHEN LOWER(fp.utm_source) LIKE 'expedia%'
                     THEN 3
                     ELSE 4
                 END
             WHEN LOWER(fp.utm_medium) = 'cpc'
                  AND LOWER(fp.channel) IN ('dis', 'som', 'eml', 'dpn')
                  AND LOWER(fp.strategy) = 'naq'
             THEN 10
             WHEN LOWER(fp.utm_medium) = 'cpc'
                  AND LOWER(fp.channel) IN ('dis', 'som', 'eml', 'dpn')
                  AND LOWER(fp.strategy) = 'act'
             THEN 58
             WHEN LOWER(fp.utm_medium) = 'cpc'
                  AND LOWER(fp.channel) IN ('dis', 'som', 'eml', 'dpn')
                  AND LOWER(fp.strategy) = 'sct'
             THEN 68
             WHEN LOWER(fp.utm_medium) = 'cpc'
                  AND LOWER(fp.channel) IN ('dis', 'som', 'eml', 'dpn')
             THEN
                  CASE
                     WHEN LOWER(fp.strategy) = 'ttt'
                     THEN 11
                     ELSE 46
                   END
             WHEN (
                  LOWER(fp.utm_medium) LIKE 'raf%'
                  OR LOWER(fp.utm_medium) LIKE 'affiliate%'
                  OR LOWER(fp.utm_medium) = 'link'
                  )
                  AND LOWER(fp.utm_campaign) LIKE 'userreferral%'
                  AND LOWER(fp.utm_medium) <> 'affiliate_link'
             THEN 12
             WHEN LOWER(fp.utm_medium) = 'bwf'
             THEN 57
             WHEN LOWER(fp.utm_medium) IN ('games')
             THEN 69
             WHEN LOWER(fp.utm_medium) IN ('social', 'glivesocial', 'twitterfb')
             THEN 15
             WHEN LOWER(fp.utm_medium) IN ('social_og_buy','app_center','facebook','twitter','pinterest')
             THEN 16
             WHEN (
                  LOWER(fp.utm_campaign) LIKE 'userreferral%'
                  OR LOWER(fp.utm_campaign) LIKE 'visitorreferral%'
                  )
                  AND (
                      LOWER(fp.utm_medium) NOT LIKE 'raf%'
                      OR LOWER(fp.utm_medium) NOT LIKE 'affiliate%'
                      OR LOWER(fp.utm_medium) <> 'link'
                      OR fp.utm_medium IS NULL
                      OR TRIM(fp.utm_medium) = ''
                      )
             THEN 17
             WHEN lower(fp.utm_medium) = 'notification'
             THEN 41
             WHEN LOWER(fp.utm_medium) = 'email'
                  AND (LOWER(fp.utm_campaign) NOT LIKE 'userreferral%' OR fp.utm_campaign IS NULL OR TRIM(fp.utm_campaign) = '')
                  THEN
                  CASE
                       WHEN LOWER(fp.utm_source) LIKE 'newsletter%' OR LOWER(fp.utm_source) LIKE 'mindstorm'
                       THEN 19
                       WHEN LOWER(fp.utm_source) LIKE 'reserve_uptown%'
                            OR LOWER(fp.utm_source) LIKE 'pc%goods%'
                            OR LOWER(fp.utm_source) LIKE 'channel%goods%'
                            OR LOWER(fp.utm_source) LIKE 'channel_goods_pc%'
                            OR LOWER(fp.utm_source) LIKE 'channel_goods_reserve_%'
                       THEN 21
                       WHEN LOWER(fp.utm_source) LIKE 'channel_occasions%'
                            OR LOWER(fp.utm_source) LIKE 'explore-city'
                            OR LOWER(fp.utm_source) LIKE 'pc_occasions%'
                       THEN 20
                       WHEN LOWER(fp.utm_source) LIKE 'groupon_getaways%'
                            OR LOWER(fp.utm_source) LIKE 'channel_getaways%'
                       THEN 22
                       WHEN LOWER(fp.utm_source) LIKE 'pc'
                            OR LOWER(fp.utm_source) LIKE 'pc_%fe'
                            OR LOWER(fp.utm_source) LIKE 'channel_pc%'
                       THEN 23
                       WHEN LOWER(fp.utm_source) LIKE 'channel_reserve_%'
                            OR LOWER(fp.utm_source) LIKE 'reserve%'
                       THEN 47
                       WHEN LOWER(fp.utm_source) LIKE 'crm%'
                            OR LOWER(fp.utm_source) LIKE '%subs_wow-deal%'
                       THEN 48
                       ELSE 24
                  END
             WHEN LOWER(fp.utm_medium) = 'affiliate_link'
                  THEN 26
             WHEN (
                       fp.utm_medium IS NULL
                       OR TRIM(fp.utm_medium) = ''
                  )
                  AND
                  (
                       fp.utm_source IS NULL
                       OR TRIM(fp.utm_source) = ''
                       OR LOWER(fp.utm_source) = 'general'
                       OR LOWER(fp.utm_source) = 'direct'
                  )
                  AND
                  (
                       fp.utm_campaign IS NULL
                       OR TRIM(fp.utm_campaign) = ''
                  )
                  THEN
                  CASE
                       WHEN LOWER(fp.referrer_domain) LIKE '%.facebook.com'
                            OR LOWER(fp.referrer_domain) LIKE '%.twitter.com'
                            OR LOWER(fp.referrer_domain) LIKE '%.meetup.com'
                            OR LOWER(fp.referrer_domain) LIKE '%.tumblr.com'
                            OR LOWER(fp.referrer_domain) LIKE '%.linkedin.com'
                            OR LOWER(fp.referrer_domain) LIKE '%.pinterest.com'
                            OR LOWER(fp.referrer_domain) LIKE '%.reddit.com'
                            OR LOWER(fp.referrer_domain) LIKE '%.stumbleupon.com'
                            OR LOWER(fp.referrer_domain) LIKE '%.digg.com'
                            OR LOWER(fp.referrer_domain) LIKE '%.youtube.com'
                            OR LOWER(fp.referrer_domain) IN ('fb.me', 't.co', 'lnkd.in', 'plus.google.com', 'bit.ly', 'su.pr')
                       THEN 25
                       WHEN fp.referrer_domain IS NOT NULL
                            AND TRIM(fp.referrer_domain) <> ''
                       THEN 26
                       ELSE 27
                 END
             ELSE 18
        END  
from
    seo_cubes.c_first_pv_sessions fp
where
    dt = '${date_value}'
