alias ga='git add '
alias gc='git commit'
alias gco='git checkout'
alias gd='git diff'
alias gdt='git difftool'
alias got='git '
alias gs='git status '
alias pull='git pull'
alias push='git push'

alias api='cd /home/apis/nykaa'
alias adminftp='cd /nykaa/adminftp'

alias nykaaproddb="mysql -h dbmaster-qa.cjmplqztt198.ap-southeast-1.rds.amazonaws.com -uapi -paU%v#sq1 -D nykaalive1"
alias nykaa_analytics=" mysql -h nykaa-analytics.nyk00-int.network -u analytics -pP1u8Sxh7kNr"
alias nykaapreproddb=" mysql -h preprod-11april2017.cjmplqztt198.ap-southeast-1.rds.amazonaws.com -u nykaalive -poh1ued3phi0uh8ooPh6 -D nykaalive1"
alias nyscripts='cd /home/ubuntu/nykaa_scripts/'
alias nyapi='cd /home/apis/nykaa'

alias solrhome='cd /opt/solr/server/scripts/cloud-scripts'
alias solrconf='cd /home/ubuntu/nykaa_solrconf'
#alias solrup='/opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost localhost:9983 -cmd upconfig -confdir /home/ubuntu/nykaa_solrconf/autocomplete -confname autocomplete'
#alias solrdown='/opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost localhost:9983 -cmd downconfig  -confname autocomplete -confdir /home/ubuntu/nykaa_solrconf/autocomplete'
#alias solrreload='curl "http://localhost:8983/solr/admin/collections?action=RELOAD&name=autocomplete"'

python /nykaa/scripts/shortcuts.py
