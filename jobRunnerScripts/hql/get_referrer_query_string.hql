SET mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

INSERT OVERWRITE TABLE ${db_name}.c_referrer_query_string PARTITION (dt = '${date_value}')


select
    user_browser_id
    ,session_id
    ,referrer_url
    ,CASE WHEN
    (referrer_domain LIKE '%.google.%' AND referrer_domain NOT LIKE 'plus%.google.com' AND referrer_domain NOT LIKE 'mail.google.%') THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE '%search.yahoo.%' THEN parse_url(referrer_url, 'QUERY', 'p')
     WHEN referrer_domain LIKE '%.bing.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE '%search.aol.%' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE '%.aolsearch.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE 'www.aol.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE '%search.lycos.%' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE 'www.lycos.%' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE '%.ask.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE '%.about.com'  THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.daum.net'  THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.eniro.se'  THEN parse_url(referrer_url, 'QUERY', 'search_word')
     WHEN referrer_domain LIKE '%search.naver.com'  THEN parse_url(referrer_url, 'QUERY', 'query')
     WHEN referrer_domain = 'www.naver.com'  THEN parse_url(referrer_url, 'QUERY', 'query')
     WHEN referrer_domain = 'cafe.naver.com' THEN parse_url(referrer_url, 'QUERY', 'query')
     WHEN referrer_domain = 'www.mamma.com'  THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE 'search%.voila.fr'  THEN parse_url(referrer_url, 'QUERY', 'rdata')
     WHEN referrer_domain = 'ricerca.virgilio.it'  THEN parse_url(referrer_url, 'QUERY', 'qs')
     WHEN referrer_domain = 'www.baidu.com'  THEN parse_url(referrer_url, 'QUERY', 'wd')
     WHEN referrer_domain LIKE 'www.yandex.%'  THEN parse_url(referrer_url, 'QUERY', 'text')
     WHEN referrer_domain LIKE 'yandex.%'  THEN parse_url(referrer_url, 'QUERY', 'text')
     WHEN referrer_domain LIKE '%search.avg.com'  THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE '%.search-results.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE '%.delta-search.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE '%.claro-search.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.search.com'  THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'szukaj.wp.pl'  THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.kvasir.no'  THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'arama.mynet.com'  THEN parse_url(referrer_url, 'QUERY', 'query')
     WHEN referrer_domain = 'nova.rambler.ru'  THEN parse_url(referrer_url, 'QUERY', 'query')
     WHEN referrer_domain = 'www.mysearchresults.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'so.360.cn' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'start.funmoods.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'start.mysearchdial.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.holasearch.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.searchya.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'start.iminent.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.webcrawler.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.safesearch.net' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.similarsitesearch.com' THEN parse_url(referrer_url, 'QUERY', 'url')
     WHEN referrer_domain = 'msxml.excite.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'searchresults.verizon.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.searchmobileonline.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www1.dlinksearch.com' THEN parse_url(referrer_url, 'QUERY', 'url')
     WHEN referrer_domain = 'advancedsearch2.virginmedia.com' THEN parse_url(referrer_url, 'QUERY', 'searchquery')
     WHEN referrer_domain = 'addons.searchalot.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'lavasoft.blekko.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'searchassist.babylon.com' THEN parse_url(referrer_url, 'QUERY',' q')
     WHEN referrer_domain = 'www.41searchengines.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.searchamong.com' THEN parse_url(referrer_url, 'QUERY', 'query')
     WHEN referrer_domain = 'www.searchgol.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.searchinq.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.searchplusnetwork.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.zzsearch.net' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'www.so.com' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain = 'search.centurylink.com' THEN parse_url(referrer_url, 'QUERY', 'origurl')
     WHEN referrer_domain = 'search.sky.com' THEN parse_url(referrer_url, 'QUERY', 'term')
     WHEN referrer_domain = 'search.mywebsearch.com' THEN parse_url(referrer_url, 'QUERY', 'searchfor')
     WHEN referrer_domain = 'search.smartaddressbar.com' THEN parse_url(referrer_url, 'QUERY', 's')
     WHEN referrer_domain = 'search.bt.com' THEN parse_url(referrer_url, 'QUERY', 'p')
     WHEN referrer_domain = 'search.yam.com'  THEN parse_url(referrer_url, 'QUERY', 'k')
     WHEN referrer_domain = 'search.bigpond.net.au' THEN parse_url(referrer_url, 'QUERY', 'searchquery')
     WHEN referrer_domain = 'search.charter.net' THEN parse_url(referrer_url, 'QUERY', 'querybox')
     WHEN referrer_domain = 'search.findwide.com' THEN parse_url(referrer_url, 'QUERY', 'k')
     WHEN referrer_domain = 'search.frontier.com' THEN parse_url(referrer_url, 'QUERY', 'origurl')
     WHEN referrer_domain = 'search.juno.com' THEN parse_url(referrer_url, 'QUERY', 'query')
     WHEN referrer_domain = 'search.maxonline.com.sg' THEN parse_url(referrer_url, 'QUERY', 'searchquery')
     WHEN referrer_domain = 'search.netzero.net' THEN parse_url(referrer_url, 'QUERY', 'query')
     WHEN referrer_domain = 'search.us.com' THEN parse_url(referrer_url, 'QUERY', 'k')
     WHEN referrer_domain LIKE 'search.%' THEN parse_url(referrer_url, 'QUERY', 'q')
     WHEN referrer_domain LIKE 'isearch.%' THEN parse_url(referrer_url, 'QUERY', 'q')
     ELSE NULL
    END as ref_query_string
from
    seo_cubes.c_first_pv_sessions
where
    dt = '${date_value}'
;
