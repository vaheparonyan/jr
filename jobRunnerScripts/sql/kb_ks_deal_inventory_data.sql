replace view sandbox.kb_ks_deal_bookings(
        country_id,
        deal_id,
        special_price,
        commission_rate,
        sum_item_count,
        sum_price_amount_gross,
        sum_item_refund,
        sum_amount_refund) as
select  DB.country_id,
        DB.deal_id,
        DB.special_price,
        DB.commission_rate,
        sum(DB.item_count) as sum_item_count,
        sum(DB.price_amount_gross) as sum_price_amount_gross,
        COALESCE(sum(DB.item_refund),0) as sum_item_refund,
        COALESCE(sum(DB.amount_refund), 0) as sum_amount_refund
from dwh_report_sec_view.v_ref_resrv_int_bookings DB
where   DB.country_id in (select country_key from dwh_mart_view.dim_country where country_code in (@parameter_country_codes)) and 
        cast(DB.created_at as date) >= cast('@parameter_start_dt' as date) and 
        cast(DB.created_at as date) < cast('@parameter_end_dt' as date) 
GROUP BY DB.country_id, DB.deal_id, DB.special_price, DB.commission_rate;

SQL_BREAK;

replace view sandbox.kb_ks_seo_deals(
        country_id,
        seo_category_id, 
        deal_id, 
        merchant_id, 
        deal_state_id, 
        deal_starts_at, 
        deal_ends_at,
        sold_deals,
        bookings, 
        revenue,
        total_sell) as
select  VDB.country_id,
        DCM.seo_category_id, 
        DC.deal_id, 
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
INNER JOIN sandbox.kb_ks_deal_bookings VDB on (VDB.deal_id = DC.deal_id)
where   VDB.country_id in (select country_key from dwh_mart_view.dim_country where country_code in (@parameter_country_codes));

SQL_BREAK;
replace view sandbox.kb_ks_seo_category_count(
        country_id,
        seo_category_id,
        seo_category_name,
        total_count, 
        total_sold_deals,
        total_bookings, 
        total_revenue)  as
select  VD.country_id,
        VC.seo_category_id,
        VC.seo_catogory_name,
        COUNT(VD.seo_category_id),
        sum(VD.sold_deals),
        sum(VD.bookings),
        sum(revenue)
FROM sandbox.kb_ks_seo_deals VD 
LEFT JOIN dwh_base_sec_view.seo_categories VC on (VC.seo_category_id = VD.seo_category_id)
where   VC.seo_category_type='CATEGORY' and 
        VC.dwh_active='1'
GROUP BY VD.country_id, VC.seo_category_id, VC.seo_catogory_name;

SQL_BREAK;

replace view sandbox.kb_ks_deal_inventory_report (
        locale,
        taxonomy_guid,
        taxonomy_category_name,
        total_count,
        total_sold_deals,
        total_bookings,
        total_revenue) as
select  CASE WHEN CC.country_id = 234 then 'en_GB' 
            WHEN CC.country_id = 83 then 'de_DE' 
            WHEN CC.country_id = 107 then 'en_IE'  
            WHEN CC.country_id = 76 then 'fr_FR' 
            WHEN CC.country_id = 110 then 'it_IT' 
            WHEN CC.country_id = 208 then 'es_ES' END,
        VATOT.taxonamy_guid,
        VATOT.taxonamy_cat_name,
        COALESCE(CC.total_count, 0) as total_count,
        COALESCE(CC.total_sold_deals, 0) as total_sold_deals,
        COALESCE(CC.total_bookings, 0) as total_bookings,
        COALESCE(CC.total_revenue, 0) as total_revenue
from sandbox.kb_ks_seo_category_count CC
LEFT JOIN sandbox.vpa_archive_id_to_guid VATOT on (cast(VATOT.archive_cat_id as INTEGER) = cast(CC.seo_category_id as INTEGER));

