set mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

INSERT OVERWRITE TABLE  ${db_name}.c_experiments_data PARTITION (dt='${date_value}')
select
    ex.session_id
    ,ex.user_browser_id
    ,ex.experiment
    ,ex.variant
    ,ex.layer
from
    seo_cubes.c_experiments_of_interest ei 
    join default.bloodhound_experiments ex on (ei.experiment = ex.experiment)
where
    ex.dt =  '${date_value}'
group by
    ex.session_id
    ,ex.user_browser_id
    ,ex.experiment
    ,ex.variant
    ,ex.layer
