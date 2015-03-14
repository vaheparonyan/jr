#!/bin/bash

JOB_XML=$1
echo JOB_XML=$JOB_XML
source ~/.bashrc

if [ "$JOB_XML" = 'load_int_bld_session_table_fcs.xml' ]; then 

    MAX_DATE=$(/usr/local/bin/tdsql -H 10.8.25.15 -u vparonyan -p LetsDoTDWB12 'select max(event_day_key) from sandbox.as_int_bld_session_1st_pv_fcs where event_day_key >= 20140201')
    echo MAX_FROM=$MAX_DATE

    DATE_FROM=$(date -d "$MAX_DATE 1 day" +%Y-%m-%d)
    echo DATE_FROM=$DATE_FROM

    DATE_TO=$(date +\%Y-\%m-\%d)
    echo DATE_TO=$DATE_TO

    echo running "/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region intl -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false"
    cd /var/groupon/apps/job_runner/jobRunnerScripts/current && /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region intl -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false
fi

if [ "$JOB_XML" = 'brightEdge.xml' ]; then 

    MAX_WEEK=$(/usr/local/bin/tdsql -H 10.20.88.35 -u vparonyan -p LetsDoTDWC12 'select max(weekkey) mod 100 from sandbox.kb_seo_brightedge')
    MAX_YEAR=$(/usr/local/bin/tdsql -H 10.20.88.35 -u vparonyan -p LetsDoTDWC12 'select max(weekkey) / 100 from sandbox.kb_seo_brightedge')
    echo MAX: $MAX_YEAR$MAX_WEEK

    DATE=$(date -d "-4 day" +%Y-%m-%d)
    echo DATE=$DATE

    WEEK_FROM=$(date -d "$DATE" +%V)
    YEAR_FROM=$(date -d "$DATE" +\%Y)
    echo FROM: $YEAR_FROM$WEEK_FROM

    if [ $MAX_YEAR = $YEAR_FROM ]; then
        while [[ $(($WEEK_FROM)) > $(($MAX_WEEK)) ]]; do
            echo Doing: $YEAR_FROM$WEEK_FROM

            DATE_TO=$(date -d "$DATE +1 day" +%Y-%m-%d);

            echo  /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE -end_day $DATE_TO -jobs $JOB_XML -date_range false
            source ~/.bashrc && cd /var/groupon/apps/job_runner/jobRunnerScripts/current && /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE -end_day $DATE_TO -jobs $JOB_XML -date_range false

            DATE=$(date -d "$DATE -7 day" +%Y-%m-%d);
            WEEK_FROM=$(date -d "$DATE" +\%V)
            echo $WEEK_FROM vs $MAX_WEEK
        done
    fi
fi

if [ "$JOB_XML" = 'seo_workflow_us.xml' ]; then
    MAX_DATE=$(/usr/local/bin/tdsql -H 10.20.88.35 -u vparonyan -p LetsDoTDWC12 'select max(event_day_key) from sandbox.as_bld_session_1st_pv_fcs where event_day_key >= 20140201');
    echo MAX_FROM=$MAX_DATE

    DATE_FROM=$(date -d "$MAX_DATE 1 day" +%Y-%m-%d)
    echo DATE_FROM=$DATE_FROM

    DATE_TO=$(date +\%Y-\%m-\%d)
    echo DATE_TO=$DATE_TO

    echo running "/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false"
    cd /var/groupon/apps/job_runner/jobRunnerScripts/current && /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false
fi

if [ "$JOB_XML" = 'seo_na_hive_cube.xml' ]; then
    MAX_DATE=$(/usr/local/bin/hive -e 'use seo_cubes; show partitions c_na_data_cube;' | tail -1 | cut -c 4-13);
    echo MAX_FROM=$MAX_DATE

    DATE_FROM=$(date -d "$MAX_DATE 1 day" +%Y-%m-%d)
    echo DATE_FROM=$DATE_FROM

    DATE_TO=$(date +\%Y-\%m-\%d)
    echo DATE_TO=$DATE_TO

    echo running "/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false"
    cd /var/groupon/apps/job_runner/jobRunnerScripts/current && /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false
fi


if [ "$JOB_XML" = 'page_performance.xml' ]; then
    MAX_DATE=$(/usr/local/bin/hive -e 'use clive; show partitions weblog_performance;' | tail -1 | cut -c 4-13);
    echo MAX_FROM=$MAX_DATE

    DATE_FROM=$(date -d "$MAX_DATE 1 day" +%Y-%m-%d)
    echo DATE_FROM=$DATE_FROM

    DATE_TO=$(date +\%Y-\%m-\%d)
    echo DATE_TO=$DATE_TO

    echo running "/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false"
    cd /var/groupon/apps/job_runner/jobRunnerScripts/current && /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false
fi


if [ "$JOB_XML" = 'getSiteMaps.xml' ]; then
    MAX_DATE=$(/usr/local/bin/tdsql -H 10.20.88.35 -u vparonyan -p LetsDoTDWC12 'select max(datekey) from sandbox.kb_sitemapurls where datekey >= 20140901');
    if [ -z "$VAR" ]; then
        MAX_DATE=$(date -d "-1 day" +%Y-%m-%d)
    fi
    
    echo MAX_FROM=$MAX_DATE

    DATE_FROM=$(date -d "$MAX_DATE 1 day" +%Y-%m-%d)
    echo DATE_FROM=$DATE_FROM

    DATE_TO=$(date -d "$DATE_FROM 1 day" +\%Y-\%m-\%d)
    echo DATE_TO=$DATE_TO

    echo running "/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false"
    cd /var/groupon/apps/job_runner/jobRunnerScripts/current && /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false
fi

if [ "$JOB_XML" = 'kb_indexed_pages.xml' ]; then
    DATE_FROM=$(date +\%Y-\%m-\%d)
    echo DATE_FROM=$DATE_FROM

    DATE_TO=$(date -d "$DATE_FROM 1 day" +%Y-%m-%d)
    echo DATE_TO=$DATE_TO

    echo running "/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false"
    cd /var/groupon/apps/job_runner/jobRunnerScripts/current && /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false
fi

if [ "$JOB_XML" = 'kb_leftover_deals.xml' ]; then
    DATE_FROM=$(date +\%Y-\%m-\%d)
    echo DATE_FROM=$DATE_FROM

    DATE_TO=$(date -d "$DATE_FROM 1 day" +%Y-%m-%d)
    echo DATE_TO=$DATE_TO

    echo running "/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false"
    cd /var/groupon/apps/job_runner/jobRunnerScripts/current && /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false
fi

if [ "$JOB_XML" = 'kb_daily_churn.xml' ]; then
    DATE_FROM=$(date +\%Y-\%m-\%d)
    echo DATE_FROM=$DATE_FROM

    DATE_TO=$(date -d "$DATE_FROM 1 day" +%Y-%m-%d)
    echo DATE_TO=$DATE_TO

    echo running "/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false"
    cd /var/groupon/apps/job_runner/jobRunnerScripts/current && /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false
fi

if [ "$JOB_XML" = 'agml_report.xml' ]; then
    MAX_DATE=$(/usr/local/bin/tdsql -H 10.20.88.35 -u vparonyan -p LetsDoTDWC12 'select max(datekey) from sandbox.kb_agml_merchants_report where datekey >= 20140901');
    if [ -z "$VAR" ]; then
        MAX_DATE=$(date -d "-1 day" +%Y-%m-%d)
    fi
    
    echo MAX_FROM=$MAX_DATE

    DATE_FROM=$(date -d "$MAX_DATE 1 day" +%Y-%m-%d)
    echo DATE_FROM=$DATE_FROM

    DATE_TO=$(date -d "$DATE_FROM 1 day" +\%Y-\%m-\%d)
    echo DATE_TO=$DATE_TO

    echo running "/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false"
    cd /var/groupon/apps/job_runner/jobRunnerScripts/current && /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false
fi

if [ "$JOB_XML" = 'seo_deal_inventory_weekly.xml' ]; then
    MAX_DATE=$(/usr/local/bin/tdsql -H 10.20.88.35 -u vparonyan -p LetsDoTDWC12 'select max(datekey) from sandbox.kb_seo_deal_inventory_weekly where datekey >= 20140901');
    if [ -z "$VAR" ]; then
        MAX_DATE=$(date -d "-1 day" +%Y-%m-%d)
    fi
    
    echo MAX_FROM=$MAX_DATE

    DATE_FROM=$(date -d "$MAX_DATE 7 day" +%Y-%m-%d)
    echo DATE_FROM=$DATE_FROM

    DATE_TO=$(date -d "$DATE_FROM 1 day" +\%Y-\%m-\%d)
    echo DATE_TO=$DATE_TO

    echo running "/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false"
    cd /var/groupon/apps/job_runner/jobRunnerScripts/current && /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false
fi

if [ "$JOB_XML" = 'seo_deal_inventory.xml' ]; then
    MAX_DATE=$(/usr/local/bin/tdsql -H 10.20.88.35 -u vparonyan -p LetsDoTDWC12 'select max(datekey) from sandbox.kb_seo_deal_inventory where datekey >= 20140901');
    if [ -z "$VAR" ]; then
        MAX_DATE=$(date -d "-1 day" +%Y-%m-%d)
    fi
    
    echo MAX_FROM=$MAX_DATE

    DATE_FROM=$(date -d "$MAX_DATE 1 day" +%Y-%m-%d)
    echo DATE_FROM=$DATE_FROM

    DATE_TO=$(date -d "$DATE_FROM 1 day" +\%Y-\%m-\%d)
    echo DATE_TO=$DATE_TO

    echo running "/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false"
    cd /var/groupon/apps/job_runner/jobRunnerScripts/current && /usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar /var/groupon/apps/job_runner/jobRunner/current/dist/Job-runner.jar -env production -region na -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range true
fi
