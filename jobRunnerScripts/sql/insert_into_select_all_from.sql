insert into @parameter_target_db.@parameter_target_table 
    select * from @parameter_source_db.@parameter_source_table;