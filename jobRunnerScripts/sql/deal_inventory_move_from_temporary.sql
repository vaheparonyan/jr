INSERT INTO @parameter_TargetTableName (DateKey, URL, deal_count, merchant_count, merchant_with_desc) 
  select @parameter_date, a02, a03, a04, a05 from @parameter_SourceTableName;

SQL_BREAK;

drop table @parameter_SourceTableName;