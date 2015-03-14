-- in case we are re-loading, drop data for these dates
DELETE FROM sandbox.as_deals_launched_count WHERE start_date_key >= @parameter_dtkey AND start_date_key <= @parameter_end_dt ;

SQL_BREAK;

INSERT INTO sandbox.as_deals_launched_count
SELECT cast(start_date as integer) + 19000000, 
  deal_demand_channel, 
  deal_supply_channel, 
  count(*) 
FROM user_groupondw.agg_deal 
WHERE deal_demand_channel <> 'Now' 
AND deal_supply_channel <> 'Now' 
AND cast(start_date as integer) + 19000000 >= @parameter_dtkey
AND cast(start_date as integer) + 19000000 <= @parameter_end_dt
GROUP BY 1,2,3;
