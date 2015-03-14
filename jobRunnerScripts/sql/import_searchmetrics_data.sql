select " Keyword", " URL", " Position", " Trend", " Tags", " Title", " Universal Search Integrations", " Search Volume", " Traffic Index",  " CPC", " Date" 
union
SELECT Keyword, URL, Position, Trend, Tags, Title, USI, Volume, Traffic,  CPC, Date 
FROM @parameter_dbname.@parameter_table_name order by Date, Keyword, URL
INTO OUTFILE '@parameter_out_file'
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n';
