set mapred.job.queue.name=seo;
  INSERT OVERWRITE TABLE ${db_name}.boomerang_data
   PARTITION (dt='${date_value}')


   SELECT 
    e.page_type,
    e.browser,
    e.os,
    bot_flag,
    substr(p.event_time,1,10) the_date,
    MONTH(p.event_time) month,
    DAY(p.event_time) day,
    HOUR(p.event_time) time,
    count(p.t_done) tdone_count,
    min(p.t_done)/1000 tdone_min,
    max(p.t_done)/1000 tdone_max,
    avg(p.t_done)/1000 tdone_avg,
    variance(p.t_done/1000) tdone_variance,
    percentile(cast(p.t_done as bigint), "0.50")/1000 tdone_tp50,
    percentile(cast(p.t_done as bigint), "0.75")/1000 tdone_tp75,
    percentile(cast(p.t_done as bigint), "0.95")/1000 tdone_tp95,
    percentile(cast(p.t_done as bigint), "0.99")/1000 tdone_tp99,
    count(p.t_resp) tresp_count,
    min(p.t_resp)/1000 tresp_min,
    max(p.t_resp)/1000 tresp_max,
    avg(p.t_resp)/1000 tresp_avg,
    variance(p.t_resp/1000) tresp_variance,
    avg(p.t_done - p.t_resp) / 1000 js_init_avg,
    variance((p.t_done - p.t_resp)/1000) js_init_variance,
    percentile(cast(p.t_resp as bigint), "0.50")/1000 tresp_tp50,
    percentile(cast(p.t_resp as bigint), "0.75")/1000 tresp_tp75,
    percentile(cast(p.t_resp as bigint), "0.95")/1000 tresp_tp95,
    percentile(cast(p.t_resp as bigint), "0.99")/1000 tresp_tp99

    FROM  (
           SELECT t_done, t_resp, page_id, event_time
             FROM bloodhound_performance 
             WHERE dt='${date_value}'
               AND t_done >=0
               AND t_done <=60000
               AND t_resp >=0
               AND t_resp <=60000
               AND event_time !=''
               AND event_time is not null 
          ) p

    JOIN 
      (
       SELECT browser, 
              page_type, 
              os, 
              bot_flag,
              page_id, 
              user_agent,
              event_time
         FROM bloodhound_events
        WHERE 
              dt='${date_value}'
-- The following is to filter out the noise from the security test we run the 11th of every month
          AND NOT user_agent LIKE '%groupon/security/team/%'
          AND user_agent NOT LIKE 'mozilla/5.0 (windows_ u_ windows nt 5.1_ en-us_ rv_1.9) gecko/20080630 firefox/3.0'
          AND NOT page_type LIKE '<%'
          AND NOT page_type LIKE '%<%'
          AND NOT page_type LIKE '%>%'
          AND NOT page_type LIKE '%(%'
          AND NOT page_type LIKE '%)%'
          AND NOT page_type LIKE "%'%"
          AND NOT page_type LIKE '%"%'
          AND NOT page_type LIKE '%[%'
          AND NOT page_type LIKE '%]%'
          AND NOT page_type LIKE '%{%'
          AND NOT page_type LIKE '%}%'
          AND NOT page_type LIKE '%|%'
          AND NOT page_type LIKE '%~%'
          AND NOT page_type LIKE '%$%'
          AND NOT page_type IN ( '*','.','^', '`',':','\;','!','#','^-' )
          AND NOT page_type RLIKE '^[0123456789].*'
--          AND NOT page_type RLIKE '\.\./.*'
--          AND NOT page_type RLIKE '\d'
          AND NOT page_type RLIKE 'aaaaaaa.*'
          AND NOT page_type RLIKE '%.*'
          AND NOT page_type RLIKE 'www.*'
          AND user_agent NOT IN ( '~','"','}','|','{','!',' ','','*','(',')','{','$','#','[',']','^','`', "'", ".","\;","\\'",'\\"' )
          AND NOT user_agent LIKE '\;%'
          AND NOT user_agent LIKE '%>%'
          AND NOT user_agent LIKE '%<%'
          AND NOT user_agent LIKE '<%'
          AND NOT user_agent LIKE '>%'
          AND NOT user_agent LIKE '/%'
          AND NOT user_agent LIKE 'null'
          AND NOT user_agent LIKE 'ahr0cdovl2nocy5jzw56awmuy29tpze0mtmwmzcxl%'
          AND NOT user_agent LIKE 'etc/%'
          AND NOT user_agent LIKE '_etc_hosts'
          AND NOT user_agent LIKE 'boot.ini'
          AND NOT user_agent LIKE 'win.ini'
          AND NOT user_agent LIKE 'file:%'
          AND NOT user_agent LIKE '%cenzic%'
          AND NOT user_agent LIKE 'confirm%'
          AND NOT user_agent LIKE 'alert%'
          AND NOT user_agent LIKE '1%'
          AND NOT user_agent LIKE "'%"
          AND NOT user_agent LIKE ")%"
          AND NOT user_agent LIKE "]%"
          AND NOT user_agent LIKE "#%"
          AND NOT user_agent LIKE "..%"
          AND NOT user_agent LIKE 'web-inf%'
          AND NOT user_agent RLIKE '%.*'
          AND NOT user_agent RLIKE 'aaaaaa.*'
          AND NOT user_agent RLIKE "'.*"

      ) e

    ON
      p.page_id=e.page_id

    GROUP by e.page_type,
             e.browser,
             e.os,
             bot_flag,
             substr(p.event_time,1,10),
             MONTH(p.event_time),
             DAY(p.event_time),
             HOUR(p.event_time)



;
