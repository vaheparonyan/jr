drop table sandbox.kb_ks_report;

SQL_BREAK;

CREATE SET TABLE sandbox.kb_ks_report ,NO FALLBACK ,
        NO BEFORE JOURNAL,
        NO AFTER JOURNAL,
        CHECKSUM = DEFAULT,
        DEFAULT MERGEBLOCKRATIO
        (
         locale VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
         guid VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC,
         category_name VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
         city VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
         keyword_string VARCHAR(1048) CHARACTER SET UNICODE NOT CASESPECIFIC,
         search_volume INTEGER,
         average_cpc DECIMAL(5,2),
         competition DECIMAL(5,2),
         total_count INTEGER,
         total_sold_deals INTEGER,
         total_bookings INTEGER,
         total_revenue DECIMAL(17,2),
         query_pattern VARCHAR(1048) CHARACTER SET UNICODE NOT CASESPECIFIC)
   PRIMARY INDEX ( locale, keyword_string );

SQL_BREAK;

insert into sandbox.kb_ks_report(locale, guid, category_name, city, keyword_string, search_volume, average_cpc,                                             
 competition, total_count, total_sold_deals, total_bookings, total_revenue, query_pattern)   
select 
	I.locale, 
	K.guid, 
	K.category_name,
	J.city,
	I.keyword_string,
	I.search_volume, 
	I.average_cpc, 
	I.competition, 
	T.total_count, 
	T.total_sold_deals, 
	T.total_bookings, 
	T.total_revenue, 
	J.query_pattern
from sandbox.kb_ks_google_potential I
left join sandbox.kb_ks_keywords J on J.locale = I.locale and lower(I.keyword_string) = lower(J.keyword)
left join sandbox.kb_taxonomies K on K.locale = I.locale and  lower(K.friendly_name_title) =  lower(J.friendly_name_title)
left join (select locale, 
                taxonomy_guid,                                                                                                     
                sum(total_count) as total_count,                                                                                   
                sum(total_sold_deals) as total_sold_deals,                                                                        
                sum(total_bookings) as total_bookings,                                                                            
                sum(total_revenue) as total_revenue                                                                               
                from sandbox.kb_ks_deal_inventory_report group by locale, taxonomy_guid) as T 
                on T.taxonomy_guid = K.guid and
                K.locale = T.locale;
	
SQL_BREAK;
	
drop table sandbox.kb_ks_locale_medians;		
		
SQL_BREAK;

CREATE SET TABLE sandbox.kb_ks_locale_medians ,NO FALLBACK ,                                                                                                                                    
        NO BEFORE JOURNAL,                                                                                                                                                                   
        NO AFTER JOURNAL,                                                                                                                                                                    
        CHECKSUM = DEFAULT,                                                                                                                                                                  
       DEFAULT MERGEBLOCKRATIO                                                                                                                                                              
       (
        locale VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,                                                                                                                           
        median_revenue  DECIMAL(17,2),
        median_search_volume INTEGER,
        median_total_count INTEGER )                                                                                                              
   PRIMARY INDEX ( locale );
	
SQL_BREAK;
	
insert into sandbox.kb_ks_locale_medians(locale, median_revenue, median_search_volume, median_total_count)
select 	locale, MEDIAN(total_revenue), MEDIAN(search_volume), MEDIAN(total_count)
from sandbox.kb_ks_report
group by locale
where total_revenue is not null and 
	total_revenue  > 0 and
	query_pattern = '<keyword>'; 
	
SQL_BREAK;

replace view sandbox.kb_ks_report_v1 as 
select r.* 
from sandbox.kb_ks_report r
left join sandbox.kb_ks_locale_medians lm
on lm.locale = r.locale
where (r.total_revenue >= lm.median_revenue or
          (r.search_volume > lm.median_search_volume and
           r.total_count is not null and 
           r.total_count > lm.median_total_count)) and 
       query_pattern = '<keyword>';
       
SQL_BREAK;
       
replace view sandbox.kb_ks_report_v2 as 	
select r.* 
from sandbox.kb_ks_report_v1 v1
left join sandbox.kb_ks_report r
on r.guid = v1.guid and r.locale = v1.locale;	

SQL_BREAK;
	
replace view sandbox.kb_ks_report_v3 as 
SELECT * 
FROM sandbox.kb_ks_report_v2
WHERE query_pattern <> '<keyword>' qualify row_number() over (partition by locale, category_name, city order by search_volume DESC, competition, average_cpc DESC ) <= 2;

SQL_BREAK;

replace view sandbox.kb_ks_report_b30 as 
select locale, category_name, avg(search_volume) as avg_search_volume 
from sandbox.kb_ks_report_v3 
group by 1,2
qualify row_number() over (partition by locale order by avg_search_volume DESC ) <= 30;

SQL_BREAK;

drop table sandbox.kb_ks_report_final;

SQL_BREAK;

CREATE MULTISET TABLE sandbox.kb_ks_report_final ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      locale VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      guid VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC,
      category_name VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      city VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      keyword_string VARCHAR(1048) CHARACTER SET UNICODE NOT CASESPECIFIC,
      search_volume INTEGER,
      average_cpc DECIMAL(5,2),
      competition DECIMAL(5,2),
      total_count INTEGER,
      total_sold_deals INTEGER,
      total_bookings INTEGER,
      total_revenue DECIMAL(17,2),
      suggested_keyword_string VARCHAR(1048) CHARACTER SET UNICODE NOT CASESPECIFIC,
      suggested_search_volume INTEGER,
      suggested_average_cpc DECIMAL(5,2),
      suggested_competition DECIMAL(5,2),
      query_pattern VARCHAR(1048) CHARACTER SET UNICODE NOT CASESPECIFIC)
PRIMARY INDEX ( locale ,keyword_string );

SQL_BREAK;

drop table sandbox.kb_ks_report_final_tmp;

SQL_BREAK;

CREATE MULTISET TABLE sandbox.kb_ks_report_final_tmp ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      locale VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
      guid VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC,
      category_name VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      city VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
      keyword_string VARCHAR(1048) CHARACTER SET UNICODE NOT CASESPECIFIC,
      search_volume INTEGER,
      average_cpc DECIMAL(5,2),
      competition DECIMAL(5,2),
      total_count INTEGER,
      total_sold_deals INTEGER,
      total_bookings INTEGER,
      total_revenue DECIMAL(17,2),
      query_pattern VARCHAR(1048) CHARACTER SET UNICODE NOT CASESPECIFIC)
PRIMARY INDEX ( locale ,keyword_string );

SQL_BREAK;

insert into sandbox.kb_ks_report_final_tmp
select v3.*
from sandbox.kb_ks_report_b30 b30
left join sandbox.kb_ks_report_v3 v3
on b30.category_name = v3.category_name and 
   b30.locale = v3.locale;
   
SQL_BREAK;

delete  from sandbox.kb_ks_report_final_tmp where search_volume = 0;

SQL_BREAK;

insert into sandbox.kb_ks_report_final_tmp   
select v.*
from sandbox.kb_ks_report_v2 v
left join sandbox.kb_ks_report_final_tmp f on v.keyword_string = f.keyword_string and v.city = f.city and v.guid = f.guid and f.category_name = v.category_name and f.locale = v.locale
where f.guid is null and v.query_pattern <> '<keyword>' 
qualify row_number() over (partition by v.locale order by v.search_volume DESC ) <= 800 - (select count(*) from sandbox.kb_ks_report_final_tmp ft where v.locale = ft.locale); 
