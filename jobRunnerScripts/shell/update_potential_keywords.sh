#!/bin/bash

usage() {
    cat <<EOF
Usage: update_potential_keywords

Relies on having seo_categories and seo_locations tables loaded 
EOF
}

DB_HOST="tdwc"
DB_USER="vparonyan"
DB_PASSWORD="LetsDoTDWC12"
SCHEMA="sandbox"
DUMP_DIR="./dump"
GOOGLE_RESULT_TABLE_NAME="kb_google_potential_tmp"
IS_KEYWORD_SELECTOR="NO"

if [ "$1" = "KS" ]; then
  IS_KEYWORD_SELECTOR="YES"
  DB_PASSWORD="LetsDoTDWB12"
  GOOGLE_RESULT_TABLE_NAME="kb_ks_google_potential"
  DB_HOST="tdwb"
fi
echo cd shell;
cd shell;

echo $1 - IS_KEYWORD_SELECTOR = $IS_KEYWORD_SELECTOR

# make sure we can find tdsql and tdload and needed libs/modules

. ~/.bashrc
export PATH=${PATH}:/usr/local/bin/:/usr/local/lib/teradata/client/Current/tbuild/bin
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib/teradata/client/Current/tbuild/lib
export TWB_ROOT=/usr/local/lib/teradata/client/Current/tbuild

# the files containg keywords are keywords_<locale>.txt
# the supported locales are :en_US en_GB en_IE de_DE it_IT fr_FR es_ES  
echo "Sending the keywords to Google API"

if [ $IS_KEYWORD_SELECTOR = "YES" ]; then 
  declare -a locale_arr=( "en_GB" "en_IE" "de_DE" "it_IT" "fr_FR" "es_ES");
else
  declare -a locale_arr=( "en_US" "en_CA" "fr_CA" "en_GB" "en_IE" "de_DE" "it_IT" "fr_FR" "es_ES");
fi

mkdir -p ./dump ./keywords
rm -rf ./dump/* ./keywords/*
for locale in "${locale_arr[@]}"
do
  if [ $IS_KEYWORD_SELECTOR = "YES" ]; then 
    #./generate_ks_potential_keywords ${DB_HOST} ${DB_USER} ${DB_PASSWORD} $locale | bteq || exit 1
    echo tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "select keyword from sandbox.kb_ks_keywords where locale ='$locale'"
    tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "select keyword from sandbox.kb_ks_keywords where locale ='$locale'" > ./keywords/keywords_$locale.txt
  else 
    echo "Creating the potential keywords table"
    ./generate_potential_keywords ${DB_HOST} ${DB_USER} ${DB_PASSWORD} | bteq || exit 1

    if [ $locale = "en_US" ]; then 
      echo "Dumping the keywords to file"
      tdsql "select keyword from sandbox.seo_keywords_and_landings l left join sandbox.seo_locations s on s.location_id = l.location_id where s.nameFull like '%United States'" > ./keywords/keywords_$locale.txt || exit 1
      tdsql "select keyword from sandbox.seo_keywords_and_landings where location_id is null" >>  ./keywords/keywords_$locale.txt || exit 1
    elif [ $locale = "en_CA" ]; then 
      echo "Dumping the keywords to file"
      tdsql "select keyword from sandbox.seo_keywords_and_landings l left join sandbox.seo_locations s on s.location_id = l.location_id where s.nameFull like '%Canada'" > ./keywords/keywords_$locale.txt || exit 1
      tdsql "select keyword from sandbox.seo_keywords_and_landings where location_id is null" >>  ./keywords/keywords_$locale.txt || exit 1
    fi
  fi
  if [ -e ./keywords/keywords_$locale.txt ]; then
    if [ $IS_KEYWORD_SELECTOR = "YES" ]; then 
      echo python ./get_keyword_ideas.py -i ./keywords/keywords_$locale.txt -o ./dump/potential_keywords_out_$locale.txt -l $locale -t 5 || exit 1
      python ./get_keyword_ideas.py -i ./keywords/keywords_$locale.txt -o ./dump/potential_keywords_out_$locale.txt -l $locale -t 5 || exit 1
    else
      echo python ./get_keyword_stats.py -i ./keywords/keywords_$locale.txt -o ./dump/potential_keywords_out_$locale.txt -l $locale || exit 1
      python ./get_keyword_stats.py -i ./keywords/keywords_$locale.txt -o ./dump/potential_keywords_out_$locale.txt -l $locale  || exit 1
    fi
  fi
done

echo

echo "Loading to Teradata"
echo "Dropping Teradata checkpoint file if it exists"
twbrmcp ${USER}

echo 

echo "Dropping table ${SCHEMA}.${GOOGLE_RESULT_TABLE_NAME} and any error tables which might exist"
# ok if these fail, so don't exit on error
tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "DROP TABLE ${SCHEMA}.${GOOGLE_RESULT_TABLE_NAME}_Log" 2>/dev/null
tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "DROP TABLE ${SCHEMA}.${GOOGLE_RESULT_TABLE_NAME}_UV" 2>/dev/null
tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "DROP TABLE ${SCHEMA}.${GOOGLE_RESULT_TABLE_NAME}_ET" 2>/dev/null
tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "DROP TABLE ${SCHEMA}.${GOOGLE_RESULT_TABLE_NAME}" 2>/dev/null

echo 

echo "Creating table ${SCHEMA}.${GOOGLE_RESULT_TABLE_NAME}" 
tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} < ../sql/${GOOGLE_RESULT_TABLE_NAME}.ddl || exit 1

echo 

#combine all files created for each locale into one
echo cat ${DUMP_DIR}/potential_keywords_out_* > ${DUMP_DIR}/potential_keywords_out.txt
cat ${DUMP_DIR}/potential_keywords_out_* > ${DUMP_DIR}/potential_keywords_out.txt || exit 1

echo 

echo "Loading data to ${SCHEMA}.${GOOGLE_RESULT_TABLE_NAME}"
tdload -h ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} -f ${DUMP_DIR}/potential_keywords_out.txt -d "TAB" -t ${GOOGLE_RESULT_TABLE_NAME} --TargetWorkingDatabase ${SCHEMA} || exit 1 

echo 

echo "Grant public select access to table"
#tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "GRANT SELECT ON ${SCHEMA}.${GOOGLE_RESULT_TABLE_NAME} TO PUBLIC"


#create table sandbox.kb_google_potential
#(
#locale  VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
#guid  VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC,
#category_name VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC,
#keyword_string VARCHAR(1048) CHARACTER SET UNICODE NOT CASESPECIFIC,
#keyword_returned VARCHAR(1048) CHARACTER SET UNICODE NOT CASESPECIFIC,
#search_volume INTEGER, 
#average_cpc DECIMAL(5,2),
#competition DECIMAL(5,2)
#)
#primary index (locale, keyword_string);
#CREATE SET TABLE sandbox.kb_ks_locale_city ,NO FALLBACK ,
#     NO BEFORE JOURNAL,
#     NO AFTER JOURNAL,
#     CHECKSUM = DEFAULT,
#     DEFAULT MERGEBLOCKRATIO
#     (
#      locale VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
#      city VARCHAR(36) CHARACTER SET UNICODE NOT CASESPECIFIC
#      )
#insert into sandbox.kb_ks_locale_city(locale, city) 
#values ('en_GB','London');
#insert into sandbox.kb_ks_locale_city(locale, city) 
#values  ('en_GB','Cardiff');
#insert into sandbox.kb_ks_locale_city(locale, city) 
#values  ('en_GB','Glasgow');
#insert into sandbox.kb_ks_locale_city(locale, city) 
#values  ('en_GB','Manchester'); 
#insert into sandbox.kb_ks_locale_city(locale, city) 
#values  ('en_GB','Belfast');
#insert into sandbox.kb_ks_locale_city(locale, city) 
#values  ('en_GB','Birmingham');
#insert into sandbox.kb_ks_locale_city(locale, city) 
#values  ('en_GB','Edinburgh'); 
#insert into sandbox.kb_ks_locale_city(locale, city) 
#values  ('en_GB','Leeds');
#insert into sandbox.kb_ks_locale_city(locale, city) 
#values  ('en_GB','Liverpool'); 
#insert into sandbox.kb_ks_locale_city(locale, city) 
#values  ('en_GB','Derby');
#PRIMARY INDEX ( locale ,city );
#
#CREATE SET TABLE sandbox.kb_ks_keyword_template ,NO FALLBACK ,
#     NO BEFORE JOURNAL,
#     NO AFTER JOURNAL,
#     CHECKSUM = DEFAULT,
#     DEFAULT MERGEBLOCKRATIO
#     (
#      locale VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC,
#      keyword_template VARCHAR(1048) CHARACTER SET UNICODE NOT CASESPECIFIC
#      )
#PRIMARY INDEX ( locale ,keyword_template );
#insert into sandbox.kb_ks_keyword_template(locale, keyword_template) 
#values ('en_GB','<city> <keyword>');
#insert into sandbox.kb_ks_keyword_template(locale, keyword_template) 
#values ('en_GB','<keyword> <city>');
#insert into sandbox.kb_ks_keyword_template(locale, keyword_template) 
#values ('en_GB','<keyword> in <city>');
#insert into sandbox.kb_ks_keyword_template(locale, keyword_template) 
#values ('en_GB','<keyword>');


if [ $IS_KEYWORD_SELECTOR = "YES" ]; then 
  echo do nothing 
else 
  for locale in en_CA en_US
  do
    echo delete $locale from  sandbox.kb_google_potential 
    tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "delete from sandbox.kb_google_potential where locale ='$locale'";
    echo insert all into sandbox.kb_google_potential and join to sandbox.seo_keywords_and_landings for taxonnomy guid
    tdsql -H ${DB_HOST} -u ${DB_USER} -p ${DB_PASSWORD} "insert into sandbox.kb_google_potential(locale, guid, category_name, keyword_string, keyword_returned, search_volume, average_cpc, competition)
    select I.locale, J.category_seo_id, K.category_name, I.keyword_string, I.keyword_returned, I.search_volume, I.average_cpc, I.competition 
    from sandbox.kb_google_potential_tmp I
    join sandbox.seo_keywords_and_landings J on 
        I.keyword_string = J.keyword and 
        I.locale = '$locale' 
    join sandbox.kb_taxonomies K on
        K.locale = '$locale' and
        K.guid = J.category_seo_id
    where I.locale = '$locale'" || exit 1; 
  done
fi

echo "Done"

