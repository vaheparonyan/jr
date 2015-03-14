#!/bin/sh

DB_HOST=$1

# Create temporary tunnel to seo postgres
if lsof -Pi :3333 -sTCP:LISTEN -t >/dev/null ; then
    echo "Tunnel already running"
else
    echo "Tunnel not running, creating new one..."
    ssh -fN -L 3333:localhost:5432 seo-content-optimizer.snc1
fi

# Prepare kb_indexed_pages table
ssh ${DB_HOST} "psql -d seogeo -U postgres -c \"

DROP TABLE IF EXISTS kb_indexed_pages;
CREATE TABLE kb_indexed_pages (
	datekey int4,
	url text,
	domain text,
	page_type text,
	channel text,
	loc_name text,
	loc_type text,
	country_code text,
	page_category text
);

 INSERT INTO kb_indexed_pages
  SELECT TO_CHAR(NOW(), 'YYYYMMDD')::INTEGER AS datekey,
       p.url, p.domain, p.page_type, ch.channel,
       l.name AS loc_name, l.type AS loc_type, l.country_code,
       c.url AS page_category
  FROM indexed_pages i
  JOIN seo_pages p ON p.url = i.url
  JOIN channels ch ON p.channel_id = ch.id
  JOIN locations l ON l.id = p.location_id
  JOIN seo_categories c ON c.id = p.category_id
  WHERE i.deal_id = (SELECT MAX(id) FROM dealer_deals)

\""

# lets use temporary tunnel (port 3333) to dump the TSV file out to our ETL machine
psql -d seogeo -U postgres -h localhost -p 3333 -c '\COPY kb_indexed_pages to '"'/var/groupon/tmp/kb_indexed_pages.tsv'";
