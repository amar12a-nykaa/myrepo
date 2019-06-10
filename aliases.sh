alias ga='git add '
alias gc='git commit'
alias gco='git checkout'
alias gd='git diff'
alias gdt='git difftool'
alias got='git '
alias gs='git status '
alias gb='git branch '
alias pull='git pull'
alias push='git push'
alias gprs='echo "https://github.com/"`cat  .git/config  | grep git@github.com | sed "s/.*git@github.com://g" | sed "s/.git$//"`"/pulls"'

alias api='cd /home/apis/nykaa'
alias api1='cd /var/www/pds_api'
alias api2='cd /var/www/discovery_api'
alias adminftp='cd /nykaa/adminftp'
alias autopep8x='autopep8 --in-place --indent-size 2 --ignore E501,E265'

alias nykaaproddb="mysql -h dbmaster-qa.cjmplqztt198.ap-southeast-1.rds.amazonaws.com -uapi -paU%v#sq1 -D nykaalive1"
alias nykaa_analytics=" mysql -h nykaa-analytics.nyk00-int.network -u analytics -pP1u8Sxh7kNr"
alias nykaapreproddb=" mysql -h preprod-11april2017.cjmplqztt198.ap-southeast-1.rds.amazonaws.com -u nykaalive -poh1ued3phi0uh8ooPh6 -D nykaalive1"
alias mongocluster="mongo --host gludo-mongo/172.30.3.5,172.30.2.45,172.30.2.154 search"

alias nyscripts='cd /home/ubuntu/nykaa_scripts/'
alias nyapi='cd /home/apis/nykaa'
alias fp='cd /home/ubuntu/nykaa_scripts/feed_pipeline'


alias popularity="/usr/bin/python /nykaa/scripts/feed_pipeline/popularity.py"
alias catalog_pipeline="/usr/bin/python /nykaa/scripts/feed_pipeline/catalogPipelineMultithreaded.py"

alias redshift="psql --host dwhcluster.cy0qwrxs0juz.ap-southeast-1.redshift.amazonaws.com --username dwh_redshift_ro -p 5439 -d datawarehouse"

python /nykaa/scripts/shortcuts.py
