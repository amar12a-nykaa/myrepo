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

alias api='cd /home/apis/nykaa'
alias adminftp='cd /nykaa/adminftp'

alias nykaaproddb="mysql -h dbmaster-qa.cjmplqztt198.ap-southeast-1.rds.amazonaws.com -uapi -paU%v#sq1 -D nykaalive1"
alias nykaa_analytics=" mysql -h nykaa-analytics.nyk00-int.network -u analytics -pP1u8Sxh7kNr"
alias nykaapreproddb=" mysql -h preprod-11april2017.cjmplqztt198.ap-southeast-1.rds.amazonaws.com -u nykaalive -poh1ued3phi0uh8ooPh6 -D nykaalive1"
alias mongocluster="mongo --host 172.30.3.5 search"
alias nyscripts='cd /home/ubuntu/nykaa_scripts/'
alias nyapi='cd /home/apis/nykaa'
alias fp='cd /home/ubuntu/nykaa_scripts/feed_pipeline'


alias popularity="/usr/bin/python /nykaa/scripts/feed_pipeline/popularity.py"
alias catalog_pipeline="/usr/bin/python /nykaa/scripts/feed_pipeline/catalogPipeline.py"
python /nykaa/scripts/shortcuts.py
