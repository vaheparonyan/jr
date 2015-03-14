LOAD DATA INFILE '@parameter_file_name'
	REPLACE
	INTO TABLE  @parameter_dbname.@parameter_table_name
	COLUMNS TERMINATED BY ','
	OPTIONALLY ENCLOSED BY '"'
	ESCAPED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 1 LINES
	SET date = '@parameter_date';
