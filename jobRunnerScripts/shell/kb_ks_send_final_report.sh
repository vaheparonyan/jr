
locales="en_GB fr_FR de_DE es_ES it_IT"
attach_arg=""
for locale in ${locales}
do 
    echo dump report header for ${locale}
    tdsql -H 10.8.25.37 -u vparonyan -p LetsDoTDWB12 -c UTF8 "select  'locale', 'guid', 'category_name', 'city', 'keyword_string', 'search_volume', 'avarage_cpc', 'competition', 'total_count', 'total_sold_deals', 'total_bookings','total_revenue', 'query_pattern', 'l2_category' " > /tmp/kb_ks_final_report_${locale}.tsv;
    
    echo dump report for ${locale}
    tdsql -H 10.8.25.37 -u vparonyan -p LetsDoTDWB12 -c UTF8 "
                select f.*, t.category_name as l2_category 
                from (
                    select * 
                    from sandbox.kb_ks_report_final_tmp
                    where locale = '${locale}') f 
                left join sandbox.kb_taxonomies_hier h 
                    on f.guid = h.guid 
                left join sandbox.kb_taxonomies t 
                    on t.guid = h.l2_guid and t.locale = '${locale}' 
                where f.locale = '${locale}'" >> /tmp/kb_ks_final_report_${locale}.tsv;

    echo dump taxonomy header for ${locale}
    tdsql -H 10.8.25.37 -u vparonyan -p LetsDoTDWB12 -c UTF8 "
                select 
                    'locale', 'category_name', 
                    'description', 'guid', 
                    'taxonomy_guid', 'child_count', 
                    'parent', 'depth', 
                    'friendly_namePlural', 'permalink', 
                    'seo_name', 'friendly_name_short', 'friendly_name', 
                    'friendly_name_singular', 
                    'friendly_name_title',  
                    'relationships_size'
            " > /tmp/kb_ks_taxonomy_${locale}.tsv;

    echo dump taxonomy for ${locale}
    tdsql -H 10.8.25.37 -u vparonyan -p LetsDoTDWB12 -c UTF8 "
        select * from sandbox.kb_taxonomies_view where locale ='${locale}'" >> /tmp/kb_ks_taxonomy_${locale}.tsv;

    attach_arg="$attach_arg -a /tmp/kb_ks_final_report_${locale}.tsv -a /tmp/kb_ks_taxonomy_${locale}.tsv"
done

echo "GROUPON keyword selector report for $locales." | mailx $attach_arg -s "GROUPON keyword selector report for $locales." vparonyan@groupon.com
