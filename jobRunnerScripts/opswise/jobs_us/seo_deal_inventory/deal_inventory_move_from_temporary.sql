INSERT INTO sandbox.kb_seo_deal_inventory (DateKey, URL, deal_count, merchant_count, merchant_with_desc)
  select ${date_key}, url, deal_count, merchant_count, merchant_with_desc from sandbox.temp_SDI_${date_key};

drop table sandbox.temp_SDI_${date_key};
