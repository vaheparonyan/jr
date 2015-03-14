#!/bin/sh

DB_HOST=$1

# Create temporary tunnel to seo postgres
if lsof -Pi :3333 -sTCP:LISTEN -t >/dev/null ; then
    echo "Tunnel already running"
else
    echo "Tunnel not running, creating new one..."
    ssh -fN -L 3333:localhost:5432 seo-content-optimizer.snc1
fi

ssh ${DB_HOST} "psql -d seogeo -U postgres -c \"

DROP TABLE IF EXISTS kb_leftover_deals;
CREATE TABLE kb_leftover_deals (
	datekey int4,
	deal_location_id int4,
	deal_uuid text,
	merchant_uuid text,
	status text,
	url text,
	start_at text,
	end_at text,
	loc_url text,
	loc_name text,
	loc_type text,
	country_code text,
	domain text,
	deal_categories text,
	merchant_name text
);

-- Store leftover deal_ids in a temp table
CREATE TEMPORARY TABLE tmp_leftover_deals (deal_id INT NOT NULL PRIMARY KEY);
 INSERT INTO tmp_leftover_deals
	SELECT DISTINCT d.id
	  FROM deals d
	  JOIN (
	  SELECT DISTINCT available.id
		FROM (
		SELECT DISTINCT d.id
		  FROM deals d
		  JOIN deal_catalog_responses dcr ON dcr.deal_uuid = d.uuid
		  JOIN deal_api_responses api ON api.deal_uuid = d.uuid AND api.response_code = 200
		  JOIN divisions div ON dcr.division_uuid = div.uuid
		  JOIN country_data cd ON cd.country_code = div.country
		  JOIN domains dom ON dom.domain = cd.tld
		  JOIN deal_locations dl ON dl.deal_id = d.id
		  JOIN locations l ON dl.primary_location_id = l.id AND l.domain = 'com'
		WHERE (d.channel IS NULL OR d.channel = 'local')
		  AND (d.status = 'open' OR d.end_at >= (SELECT least(max(start_at), now()) FROM deals WHERE status = 'open') - INTERVAL '100 days')
		) as available
		LEFT JOIN (
		SELECT DISTINCT d.id
		  FROM seo_page_deals pd
		  JOIN deal_locations dl ON pd.deal_location_id = dl.id
		  JOIN deals d ON dl.deal_id = d.id
		  WHERE (d.channel IS NULL OR d.channel = 'local')
		) as indexed ON indexed.id = available.id
	  WHERE indexed.id IS NULL
	  ) as leftover ON leftover.id = d.id;

INSERT INTO kb_leftover_deals
  SELECT TO_CHAR(NOW(), 'YYYYMMDD')::INTEGER AS datekey,
    dl.id as deal_location_id, d.uuid AS deal_uuid, d.merchant_uuid, d.status, d.url, d.start_at, d.end_at,
    l.url AS loc_url, l.name AS loc_name, l.type AS loc_type, l.country_code, l.domain,
    dt.categories AS deal_categories,
    mds.NAME AS merchant_name
    FROM tmp_leftover_deals tmp
      JOIN deals d ON d.id = tmp.deal_id
      JOIN deal_locations dl ON d.id = dl.deal_id
      JOIN locations l ON l.ID = dl.primary_location_id
      LEFT JOIN mds_merchants mds ON mds.uuid = d.merchant_uuid
      LEFT JOIN (
        SELECT dspc.deal_id, STRING_AGG(c.singular, ',' ORDER BY RANK) AS categories
          FROM tmp_leftover_deals tmp
            JOIN deal_seo_page_categories dspc ON dspc.deal_id = tmp.deal_id
            JOIN seo_categories c ON c.id = dspc.seo_category_id AND c.url IS NOT NULL
            GROUP BY 1
       ) AS dt ON dt.deal_id = dl.deal_id;

\""

# lets use temporary tunnel (port 3333) to dump the TSV file out to our ETL machine
psql -d seogeo -U postgres -h localhost -p 3333 -c '\COPY kb_leftover_deals to '"'/var/groupon/tmp/kb_leftover_deals.tsv'";
