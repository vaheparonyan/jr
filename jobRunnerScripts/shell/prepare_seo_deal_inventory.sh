#!/bin/sh

ssh seo-content-optimizer.snc1 "psql -d seogeo -U postgres -c \"

drop table IF EXISTS vpa_seo_deal_inventory;
create table vpa_seo_deal_inventory(page_id INTEGER PRIMARY KEY,
                                    url varchar(2048),
                                    deal_count integer default 0,  
                                    merchant_count integer default 0, 
                                    merchant_with_description_count integer default 0);

insert into vpa_seo_deal_inventory 
select SP.id as page_id, SP.url as url,  COALESCE(SPS.size, '0') as deal_count
from seo_pages SP
left join seo_page_sizes SPS on SP.id = SPS.page_id
where SPS.type = 'dealt-seo-deals';

UPDATE vpa_seo_deal_inventory
SET merchant_count = MC.merchant_count  
from ( 
    select SP.id as page_id,  COALESCE(SPS.size, 0) as merchant_count
    from seo_pages SP
    left join seo_page_sizes SPS on SP.id = SPS.page_id
    where SPS.type = 'dealt-seo-merchants'
) as MC
where MC.page_id = vpa_seo_deal_inventory.page_id;

UPDATE vpa_seo_deal_inventory
SET merchant_with_description_count = UMD.description_count  
from (
    SELECT SP.id, count(*) as description_count
    FROM seo_pages SP
    JOIN seo_page_merchants SPM ON SPM.page_id = SP.id
    JOIN mds_places MP ON SPM.merchant_location_id = MP.id
    JOIN mds_place_indexable MDI ON MDI.mds_place_id = MP.id
        AND MDI.reason = 'has-description'
    GROUP BY SP.id
) as UMD
where UMD.id = vpa_seo_deal_inventory.page_id;

\""
