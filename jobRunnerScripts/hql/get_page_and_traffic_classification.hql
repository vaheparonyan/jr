set mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

INSERT OVERWRITE TABLE ${db_name}.c_page_and_traffic_classification PARTITION (dt = '${date_value}')

select
    fp.user_browser_id
    ,fp.session_id
    ,CASE WHEN fp.os in ('mac','windows','ios','android') THEN os ELSE 'Other' END
    ,CASE WHEN fp.browser IN ('ie', 'chrome', 'safari', 'firefox', 'mobile safari', 'android', 'chrome mobile', 'googlebot') THEN browser ELSE 'Other' END 
    ,CASE WHEN fp.browser in ('android','chrome mobile','mobile safari') THEN 'mobile' else 'non-mobile' END  
    ,CASE
         WHEN fp.page_type in ('homepage/index','home/index-seo','browse/deals/index','subscriptions/itier/local-zip') THEN '1.Homepage'
         WHEN fp.page_type in ('home/index','local/city') THEN '2.City Page'
         WHEN fp.page_type = 'local/city/category' THEN '3.City+Cat SERP'
         WHEN fp.page_type = 'merchant/show' THEN '4./biz'
         WHEN fp.page_channel = 'getaways'  THEN '6.Getaways'
         WHEN fp.page_channel = 'goods' THEN '7.Goods'
         WHEN (fp.page_type = 'deals/show' and  ed.widget_name is NOT NULL ) THEN '5.Local Deal Exp'
         WHEN (page_type = 'deals/show' and ed.widget_name IS NULL  ) THEN '5.Local Deal Active'
         WHEN (fp.page_type like '%coupons%' or fp.page_type like '%touch-coupons%') THEN '8.Coupons'
         WHEN fp.page_type in ('stores/show_brand','stores/index') THEN '9.Brand'
         ELSE '10.Others' END
    ,CASE 
        WHEN 
            (fp.page_type in ('homepage/index','home/index-seo','subscriptions/itier/local-zip')
            or (fp.page_type = 'channel/show' and fp.page_channel = 'getaways')
            or (fp.page_type = 'channel/show' and fp.page_channel  = 'goods')
            or (fp.page_type = 'local/city'))
            THEN 'Brand'
        WHEN fp.page_type = 'merchant/show' THEN 'Local - Merchant'
        WHEN 
            ((fp.page_type like 'local/%' and fp.page_type != 'local/city')
            OR (fp.page_type = 'merchant/merchant_list'))
            THEN 'Merchant - Geocat'
        WHEN ((fp.page_type = 'hotels/show' or fp.page_type = 'deals/show')   
            AND fp.page_channel = 'getaways')
            THEN 'Getaways - Merchant'
        WHEN fp.page_type like 'seo/getaways/%' THEN  'Getaways - Geocat'
        WHEN ((fp.page_type = 'deals/show' and fp.page_channel = 'goods') 
            or fp.page_type = 'brand/show')
            THEN 'Goods - Product'
        WHEN fp.page_type = 'goods/category' THEN 'Goods - Category'
        WHEN fp.page_type = 'coupons/store/show' THEN 'Coupons - Merchant'
        WHEN fp.page_type like 'coupons%' and fp.page_type != 'coupons/store/show' THEN 'Coupons - Category'
        ELSE 'Other' END
from    
    seo_cubes.c_first_pv_sessions fp
    left outer join seo_cubes.c_expired_deal_pages ed on (fp.page_id = ed.page_id and fp.dt = ed.dt)
where
    fp.dt = '${date_value}'
