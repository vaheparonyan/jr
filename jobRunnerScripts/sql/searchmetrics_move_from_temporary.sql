INSERT INTO @parameter_TargetTableName (DateKey, Keyword, URL, Pos, Trend, Tags, Ttl, USI, Volume, Traffic, CPC) 
  select @parameter_date, a01, a02, a03, a04, a05, a06, a07, a08, a09, a10 from @parameter_SourceTableName;

SQL_BREAK;

drop table @parameter_SourceTableName;