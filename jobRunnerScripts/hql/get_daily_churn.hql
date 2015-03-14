SET mapred.job.queue.name=seo;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;

CREATE TABLE IF NOT EXISTS ${db_name}.kb_daily_churn
 (
	url STRING,
	domain STRING,
	page_type STRING,
	channel STRING,
	loc_name STRING,
	loc_type STRING,
	country_code STRING,
	page_category STRING,
	datekey INT,
    top_level_location STRING,
	change_type STRING
 ) PARTITIONED BY (change_date STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',';

-- Store pages removed from the index since the last dealer run

INSERT OVERWRITE TABLE ${db_name}.kb_daily_churn PARTITION (change_date = '${date_value}')
SELECT d1.*, split(d1.url,'\\/')[1] as top_level_location, 'removed' AS change_type
FROM ${db_name}.kb_indexed_pages d1
LEFT OUTER JOIN (
		SELECT d2.* FROM ${db_name}.kb_indexed_pages d2
		WHERE d2.datekey = ${date_key}) d2 ON d1.url = d2.url
JOIN (
		SELECT MAX(datekey) AS datekey FROM ${db_name}.kb_indexed_pages
		WHERE datekey < ${date_key}) prev ON d1.datekey = prev.datekey
WHERE d2.url IS NULL;

-- Store pages added to the index since the last dealer run

INSERT INTO TABLE ${db_name}.kb_daily_churn PARTITION (change_date = '${date_value}')
SELECT d2.*, split(d2.url,'\\/')[1] as top_level_location, 'added' AS change_type
FROM ${db_name}.kb_indexed_pages d2 
LEFT OUTER JOIN (
		SELECT d1.* FROM ${db_name}.kb_indexed_pages d1
		JOIN (
				SELECT MAX(datekey) AS datekey FROM ${db_name}.kb_indexed_pages 
				WHERE datekey < ${date_key}) prev ON d1.datekey = prev.datekey
		) d1 ON d1.url = d2.url
WHERE d2.datekey = ${date_key} AND d1.url IS NULL;