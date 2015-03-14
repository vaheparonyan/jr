set :stages, %w(staging production)
set :default_stage, "staging"

require 'capistrano/ext/multistage'

set :application, "jobRunner"

set :scm, :git
set :repository, "git@github.groupondev.com:seo/jobRunner.git"
set :scm_passphrase, ""

set :user, "juicer"
set :group, "juicer"
set :use_sudo, false

set :deploy_to,   "/var/groupon/apps/job_runner/jobRunner"
set :keep_releases,  5

namespace :deploy do
  task :restart_apps, :roles => :app do
    run "cd #{release_path} && ant"
    #run "sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8000"
    #run "export LD_LIBRARY_PATH=/usr/local/lib:/usr/lib:/usr/local/lib64:/usr/lib64:$LD_LIBRARY_PATH && ant"

    #run "cd #{release_path} && export JAVA_HOME=/packages/encap/java-1.7.0_65 && export LD_LIBRARY_PATH=/usr/local/lib:/usr/lib:/usr/local/lib64:/usr/lib64:$LD_LIBRARY_PATH && NODE_ENV=staging ./node_modules/forever/bin/forever start -a -l ./forever.log -o ./out.log -e ./err.log app.js"
  end
end

after "deploy", "deploy:restart_apps"