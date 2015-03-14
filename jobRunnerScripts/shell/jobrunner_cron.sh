#!/bin/bash

MAX_DATE=$(/usr/local/bin/tdsql -H tdwb -u vparonyan -p LetsDoTDWB12 'select max(event_day_key) from sandbox.as_int_bld_session_1st_pv_fcs where event_day_key >= 20140201')
echo MAX_FROM=$MAX_DATE

DATE_FROM=$(date -d "$MAX_DATE 1 day" +%Y-%m-%d)
echo DATE_FROM=$DATE_FROM

DATE_TO=$(date -d "1 day" +\%Y-\%m-\%d)
echo DATE_TO=$DATE_TO

JOB_XML=load_int_bld_session_table_fcs.xml
echo JOB_XML=$JOB_XML

source ~/.bashrc 
echo sourcing bashrc

cd /var/groupon/job_runner/jobRunnerScripts
echo cd to jobRunnerScripts

echo running "/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar ../jobRunner/dist/Job-runner.jar -env production -region intl -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false"
/usr/local/bin/java -Djava.security.egd=file:/dev/./urandom -jar ../jobRunner/dist/Job-runner.jar -env production -region intl -start_day $DATE_FROM -end_day $DATE_TO -jobs $JOB_XML -date_range false

