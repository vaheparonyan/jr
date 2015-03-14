SET mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;


INSERT OVERWRITE TABLE seo_cubes.c_session_orders_tmp PARTITION (dt = '${date_value}')

select
     COALESCE(fo.order_date_key,-99)
    ,COALESCE(fo.tracking_cookie,'none')
    ,COALESCE(fo.order_id,-99)
    ,fo.src_created_date
    ,unix_timestamp(fo.src_created_date)
    ,du.first_purchase_date
    ,max(du.user_key)
    ,sum(fc.sale_amount)
    ,max(do.cost_to_groupon)
    ,max(do.cost_to_user)
FROM
    td_backup.fact_orders fo
    join td_backup.fact_collections fc on (COALESCE(fo.order_id,-99) = fc.order_id and COALESCE(fo.tracking_cookie,'none') = fc.tracking_cookie and fo.ds = fc.ds)
    join td_backup.dim_deal_option do on (COALESCE(do.deal_option_key,-99) = fc.deal_option_key)
    join td_backup.dim_user du on (COALESCE(du.user_key,-99) = fc.user_key)

where
    fo.ds = ${date_key}
    and fc.ds = ${date_key}
    and fo.order_date_key = ${date_key}
    and fc.tracking_cookie is NOT NULL
group by
     COALESCE(fo.order_date_key,-99)
    ,COALESCE(fo.tracking_cookie,'none')
    ,COALESCE(fo.order_id,-99)
    ,fo.src_created_date
    ,unix_timestamp(fo.src_created_date)
    ,du.first_purchase_date
;
