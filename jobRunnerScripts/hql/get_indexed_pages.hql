SET mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

CREATE TABLE IF NOT EXISTS ${db_name}.kb_indexed_pages
 (
	url STRING,
	domain STRING,
	page_type STRING,
	channel STRING,
	loc_name STRING,
	loc_type STRING,
	country_code STRING,
	page_category STRING
 ) PARTITIONED BY (datekey INT)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',';

-- Two-step TSV load into partitioned table
DROP TABLE ${db_name}.kb_indexed_pages_tmp;
CREATE TABLE ${db_name}.kb_indexed_pages_tmp
 (
	datekey INT,
	url STRING,
	domain STRING,
	page_type STRING,
	channel STRING,
	loc_name STRING,
	loc_type STRING,
	country_code STRING,
	page_category STRING
 ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',';

-- First, load TSV file to a non-partitioned temp table
LOAD DATA LOCAL INPATH '/var/groupon/tmp/kb_indexed_pages.tsv' INTO TABLE ${db_name}.kb_indexed_pages_tmp;

-- Then copy from temp table to partitioned table using dynamic partitioning
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ${db_name}.kb_indexed_pages PARTITION (datekey)
SELECT url, domain, page_type, channel, loc_name, loc_type, country_code, page_category, datekey
FROM ${db_name}.kb_indexed_pages_tmp;