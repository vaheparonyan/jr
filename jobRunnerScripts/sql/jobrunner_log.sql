insert into knowledge_base_v1.jobs_log(env, region, start_date, end_date, file_name, machine_name, user_name, start_time, end_time, status, error_msg, job_name, job_content, parent, id) 
values ('@parameter_env', 
'@parameter_region', 
'@parameter_start_date', 
'@parameter_end_date', 
'@parameter_file_name', 
'@parameter_machine_name',
'@parameter_user_name',  
'@parameter_start_time', 
'@parameter_end_time', 
'@parameter_status', 
"@parameter_error_msg", 
'@parameter_job_name', 
"@parameter_job_content",
"@parameter_parent",
"@parameter_id")
ON DUPLICATE KEY UPDATE 
end_time='@parameter_end_time', 
status='@parameter_status', 
error_msg="@parameter_error_msg";

