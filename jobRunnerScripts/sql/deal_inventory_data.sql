replace view sandbox.vpa_deal_bookings(
        deal_id,
        special_price,
        commission_rate,
        sum_item_count,
        sum_price_amount_gross,
        sum_item_refund,
        sum_amount_refund) as
select  DB.deal_id,
        DB.special_price,
        DB.commission_rate,
        sum(DB.item_count) as sum_item_count,
        sum(DB.price_amount_gross) as sum_price_amount_gross,
        CASE WHEN sum(DB.item_refund) is null THEN 0 ELSE sum(DB.item_refund) END as sum_item_refund,
        CASE WHEN sum(DB.amount_refund) is null THEN 0 ELSE sum(DB.amount_refund) END as sum_amount_refund
from dwh_report_sec_view.v_ref_resrv_int_bookings DB
where   DB.country_id=(select country_key from dwh_mart_view.dim_country  where country_code='@parameter_country_code') and 
        cast(DB.created_at as date) >= cast('@parameter_start_dt' as date) and 
        cast(DB.created_at as date) < cast('@parameter_end_dt' as date) 
GROUP BY DB.deal_id, DB.special_price, DB.commission_rate;

SQL_BREAK;

replace view sandbox.vpa_seo_deals(
        seo_category_id, 
        deal_id, 
        country_id, 
        merchant_id, 
        deal_state_id, 
        deal_starts_at, 
        deal_ends_at,
        sold_deals,
        bookings, 
        revenue,
        total_sell) as
select  DCM.seo_category_id, 
        DC.deal_id, 
        DC.country_id, 
        DC.merchant_id, 
        DC.deal_state_id, 
        DC.deal_starts_at, 
        DC.deal_ends_at,
        VDB.sum_item_count,
        VDB.sum_item_count - VDB.sum_item_refund,
        (sum_price_amount_gross-sum_amount_refund) * VDB.commission_rate,
        sum_price_amount_gross-sum_amount_refund
from dwh_mart_view.deals_core DC
INNER JOIN dwh_base_sec_view.deal_seo_categorie_map DCM on (DCM.deal_id = DC.deal_id)
INNER JOIN sandbox.vpa_deal_bookings VDB on (VDB.deal_id = DC.deal_id)
where   DC.country_id=(select country_key from dwh_mart_view.dim_country  where country_code='@parameter_country_code');

SQL_BREAK;

replace view sandbox.vpa_seo_category_count(
        seo_category_id,
        seo_category_name,
        total_count, 
        total_sold_deals,
        total_bookings, 
        total_revenue)  as
select  VC.seo_category_id,
        VC.seo_catogory_name,
        COUNT(VD.seo_category_id),
        sum(VD.sold_deals),
        sum(VD.bookings),
        sum(revenue)
FROM dwh_base_sec_view.seo_categories VC 
LEFT JOIN sandbox.vpa_seo_deals VD on (VC.seo_category_id = VD.seo_category_id)
where   VC.seo_category_type='CATEGORY' and 
        VC.dwh_active='1'
GROUP BY VC.seo_category_id, VC.seo_catogory_name; 

SQL_BREAK;

replace view sandbox.vpa_seo_deal_inventory_report (
        taxonamy_guid,
        taxonamy_category_name,
        total_count,
        total_sold_deals,
        total_bookings,
        total_revenue) as
select  VATOT.taxonamy_guid,
        VATOT.taxonamy_cat_name,
        CASE WHEN CC.total_count is null THEN 0 ELSE CC.total_count END as total_count,
        CASE WHEN CC.total_sold_deals is null THEN 0 ELSE CC.total_sold_deals END as total_sold_deals,
        CASE WHEN CC.total_bookings is null THEN 0 ELSE CC.total_bookings END as total_bookings,
        CASE WHEN CC.total_revenue is null THEN 0 ELSE CC.total_revenue END as total_revenue
from sandbox.vpa_seo_category_count CC
LEFT JOIN sandbox.vpa_archive_id_to_guid VATOT on (cast(VATOT.archive_cat_id as INTEGER) = cast(CC.seo_category_id as INTEGER));
