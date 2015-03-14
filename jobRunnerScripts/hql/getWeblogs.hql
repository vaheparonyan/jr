set mapred.job.queue.name=seo;  
INSERT OVERWRITE TABLE ${db_name}.weblog_performance
   PARTITION (dt='${date_value}')
     select
       if (LENGTH(w.path) = 1,w.path,
          CONCAT("/", SUBSTR(w.path,1,LENGTH(w.path) -1))), 
       w.status,
       w.googlebot,
       w.first_visit,
       w.request_type,
       w.request_date,
       HOUR(w.request_time) hour,
       count(w.path) path_count,
       min(w.response_time) response_time_min,
       max(w.response_time) response_time_max,
       avg(w.response_time) response_time_avg,
       variance(w.response_time) response_time_variance,
       percentile_approx(cast(w.response_time AS double), 0.50) response_time_tp50,
       percentile_approx(cast(w.response_time AS double), 0.75) response_time_tp75,
       percentile_approx(cast(w.response_time AS double), 0.95) response_time_tp95,
       percentile_approx(cast(w.response_time AS double), 0.99) response_time_tp99

   from (select 
       if (instr(request,"/") is null, null,
           if (instr(request,"/") = 0, null,
                if (split(request,"/")[1] is null,
                  if (length(request) > 1, null, "/"),
                     CONCAT( split(request,"/")[1], "/"))) ) AS path,
                status,
       if (user_agent LIKE '%Googlebot%',1, 0) AS googlebot,
       first_visit,
       request_type,
       response_time,
       request_time,
       from_unixtime(cast(substr(epoch_time,0,10) AS bigint), "yyyy-MM-dd") AS request_date
                
           
        from weblogs

        where  dt="${date_value}" 
          and  request not like '%/autocomplete%' 
          and  request NOT Like '%.jpg'
          and  request NOT Like '%.png'
          and  request NOT Like '%.gif'
          and  status >=200
          and  status <=600
        ) w

    JOIN (select path from clive.interesting_paths) ip
    ON ip.path=w.path

   group by 
       w.request_date,
       HOUR(w.request_time), 
       w.path,
       w.status,
       w.googlebot,
       w.first_visit,
       w.request_type         





; 
