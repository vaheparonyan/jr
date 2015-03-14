INSERT INTO sandbox.kb_ks_keywords                                                                                                                                                          
select 
  'en_US',
  T.name as keyword, 
  T.name as friendly_name_title,
  null,
  '<keyword>' as query_pattern 
from sandbox.seo_categories_with_names T;

SQL_BREAK;

INSERT INTO sandbox.kb_ks_keywords                                                                                                                                                          
select 
 'en_US', 
 C.name || ' ' || T.name as keyword, 
 T.name as friendly_name_title, 
 C.name,
 '<city> <keyword>' as query_pattern 
from sandbox.seo_categories_with_names T 
full outer join sandbox.seo_locations C
where C.name is not null and C.nameFull like '%United States';

SQL_BREAK;

INSERT INTO sandbox.kb_ks_keywords                                                                                                                                                          
select 
 'en_US', 
 C.name || ' ' || T.name as keyword, 
 T.name as friendly_name_title, 
 C.name,
 '<keyword> <city>' as query_pattern 
from sandbox.seo_categories_with_names T 
full outer join sandbox.seo_locations C
where C.name is not null and C.nameFull like '%United States';

SQL_BREAK;

INSERT INTO sandbox.kb_ks_keywords                                                                                                                                                          
select 
 'en_US', 
 C.name || ' ' || T.name as keyword, 
 T.name as friendly_name_title, 
 C.name,
 '<keyword> in <city>' as query_pattern 
from sandbox.seo_categories_with_names T 
full outer join sandbox.seo_locations C
where C.name is not null and C.nameFull like '%United States';

SQL_BREAK;


INSERT INTO sandbox.kb_ks_keywords                                                                                                                                                          
select 
  'en_CA',
  T.name as keyword, 
  T.name as friendly_name_title,
  null,
  '<keyword>' as query_pattern 
from sandbox.seo_categories_with_names T;
                  
SQL_BREAK;

INSERT INTO sandbox.kb_ks_keywords                                                                                                                                                          
select 
 'en_CA', 
 C.name || ' ' || T.name as keyword, 
 T.name as friendly_name_title, 
 C.name,
 '<city> <keyword>' as query_pattern 
from sandbox.seo_categories_with_names T 
full outer join sandbox.seo_locations C
where C.name is not null and C.nameFull like '% Canada';

SQL_BREAK;

INSERT INTO sandbox.kb_ks_keywords                                                                                                                                                          
select 
 'en_CA', 
 C.name || ' ' || T.name as keyword, 
 T.name as friendly_name_title, 
 C.name,
 '<keyword> <city>' as query_pattern 
from sandbox.seo_categories_with_names T 
full outer join sandbox.seo_locations C
where C.name is not null and C.nameFull like '% Canada';

SQL_BREAK;

INSERT INTO sandbox.kb_ks_keywords                                                                                                                                                          
select 
 'en_CA', 
 C.name || ' ' || T.name as keyword, 
 T.name as friendly_name_title, 
 C.name,
 '<keyword> in <city>' as query_pattern 
from sandbox.seo_categories_with_names T 
full outer join sandbox.seo_locations C
where C.name is not null and C.nameFull like '% Canada';