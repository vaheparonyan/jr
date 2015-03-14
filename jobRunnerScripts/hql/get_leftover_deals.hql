SET mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

CREATE TABLE IF NOT EXISTS ${db_name}.kb_leftover_deals
 (
	deal_location_id INT,
	deal_uuid STRING,
	merchant_uuid STRING,
	status STRING,
	url STRING,
	start_at STRING,
	end_at STRING,
	loc_url STRING,
	loc_name STRING,
	loc_type STRING,
	country_code STRING,
	domain STRING,
	deal_categories STRING,
	merchant_name STRING
 ) PARTITIONED BY (datekey INT)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',';

-- Two-step TSV load into partitioned table
DROP TABLE ${db_name}.kb_leftover_deals_tmp;
CREATE TABLE ${db_name}.kb_leftover_deals_tmp
 (
	datekey INT,
	deal_location_id INT,
	deal_uuid STRING,
	merchant_uuid STRING,
	status STRING,
	url STRING,
	start_at STRING,
	end_at STRING,
	loc_url STRING,
	loc_name STRING,
	loc_type STRING,
	country_code STRING,
	domain STRING,
	deal_categories STRING,
	merchant_name STRING
 ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',';

-- First, load TSV file to a non-partitioned temp table
LOAD DATA LOCAL INPATH '/var/groupon/tmp/kb_leftover_deals.tsv' INTO TABLE ${db_name}.kb_leftover_deals_tmp;

-- Then copy from temp table to partitioned table using dynamic partitioning
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ${db_name}.kb_leftover_deals PARTITION (datekey)
SELECT deal_location_id, deal_uuid, merchant_uuid, status, url, start_at, end_at, loc_url, loc_name, loc_type, country_code, domain, deal_categories, merchant_name, datekey
FROM ${db_name}.kb_leftover_deals_tmp;