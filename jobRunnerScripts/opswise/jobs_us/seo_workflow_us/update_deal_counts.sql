-- in case we are re-loading, drop data for these dates
DELETE FROM sandbox.as_deals_launched_count WHERE start_date_key >= ${date_key} AND start_date_key <= ${end_date_key} ;


INSERT INTO sandbox.as_deals_launched_count
SELECT cast(start_date as integer) + 19000000, 
  deal_demand_channel, 
  deal_supply_channel, 
  count(*) 
FROM user_groupondw.agg_deal 
WHERE deal_demand_channel <> 'Now' 
AND deal_supply_channel <> 'Now' 
AND cast(start_date as integer) + 19000000 >= ${date_key}
AND cast(start_date as integer) + 19000000 <= ${end_date_key}
GROUP BY 1,2,3;
