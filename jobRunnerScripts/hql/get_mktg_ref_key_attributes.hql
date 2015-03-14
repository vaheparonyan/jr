set mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;


INSERT OVERWRITE TABLE ${db_name}.c_mktg_ref_key_attributes PARTITION (dt = '${date_value}')

select
    rk.user_browser_id
    ,rk.session_id    
    ,rk.mktg_ref_key
    ,ra.traffic_type
    ,ra.traffic_source
    ,ra.traffic_sub_source
from
    seo_cubes.c_mktg_ref_key_data rk
    left outer join td_backup.ref_attr_class ra on (rk.mktg_ref_key = ra.ref_attr_class_key)
where
    rk.dt = '${date_value}'

