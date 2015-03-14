set :repo_url,  "git@github.groupondev.com:seo/jobRunnerScripts.git"
ask :branch, proc { `git rev-parse --abbrev-ref HEAD`.chomp }.call

set :deploy_to,   "/var/groupon/seo_opswise"
set :keep_releases,  5

task :copy_conf_files do
  on roles(:app) do |host|
    execute "cp #{release_path}/opswise/conf/.odbc.ini /home/juicer/"
    execute "cp #{release_path}/opswise/conf/.zombierc /home/juicer/"
    execute "cp #{release_path}/opswise/conf/.tdsqlrc_us /home/juicer/.tdsqlrc"
  end
end

after 'deploy:finished', 'copy_conf_files'

namespace :deploy do
  task :start do ; end
  task :stop do ; end
  task :restart do ; end
end
