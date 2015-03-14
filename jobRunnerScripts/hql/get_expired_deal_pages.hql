set mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

INSERT OVERWRITE TABLE ${db_name}.c_expired_deal_pages PARTITION (dt = '${date_value}')

select
    bw.page_id
    ,bw.widget_name
from
(
select
    page_id
    ,widget_name
from
    default.bloodhound_widgets  
where
    dt = '${date_value}'
    and widget_name in ('dealunavailablesoldout' , 'dealunavailableexpired')
) bw
group by
    bw.page_id
    ,bw.widget_name
