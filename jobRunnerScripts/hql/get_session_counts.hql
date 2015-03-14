set mapred.job.queue.name=seo;
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

INSERT OVERWRITE TABLE ${db_name}.c_session_counts PARTITION (dt = '${date_value}')

SELECT
    be.event_day
    ,COALESCE(be.user_browser_id,'none')
    ,CASE WHEN SUBSTR(COALESCE(be.session_id,'none'),1,32) not like '%-%' THEN SUBSTR(COALESCE(be.session_id,'none'),1,32) else substr(COALESCE(be.session_id,'none'),1,36) end
    ,COUNT(*)
    ,MIN(unix_timestamp(event_time))
    ,MAX(unix_timestamp(event_time))
FROM
    default.bloodhound_events be
WHERE
    be.event = 'pageview'
    and be.dt ='${date_value}'
    and be.page_country in ('US','CA') 
GROUP BY
    be.event_day
    ,COALESCE(be.user_browser_id,'none')
    ,CASE WHEN SUBSTR(COALESCE(be.session_id,'none'),1,32) not like '%-%' THEN SUBSTR(COALESCE(be.session_id,'none'),1,32) else substr(COALESCE(be.session_id,'none'),1,36) end
;
