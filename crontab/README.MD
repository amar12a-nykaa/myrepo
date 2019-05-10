# Crontab 

## What is the purpose of this directory ? 
Generally to configure crons in a machine, people use commands like `crontab -e` and `crontab -l`. This works but the crons remain in the machine and in case machine is gone, you will loose all the job setup in crontab. To remedy this situation, we need to version control our crons too. Hence this directory. 

## How does it work 

In a machine, every file in directory `/etc/cron.d/` is run every hour by cron.
So, all we need to do is add a symlink from a file in cron.d directory to file that we are maintaining in this repository.
 
There are good chance that these symlinks are already created in EC2 machine that you are working on. 

Run this to see if crontab is setup in your machine
`ls -l /etc/cron.d`

Sample Response
```
-rw-r--r-- 1 root root  589 Jul 16  2014 mdadm
lrwxrwxrwx 1 root root   30 Sep 14  2017 nykaa -> /nykaa/scripts/crontab/preprod
-rw-r--r-- 1 root root  670 Mar  1  2016 php
-rw-r--r-- 1 root root  191 Jan 13  2017 popularity-contest
-rw-r--r-- 1 root root 2482 Mar 28  2017 sendmail
````
## What are different files ? 

In this directory, you will see one file per environment.
* admin - Production Environment
* preprod - Pre Production Environment
* qa - QA Environment




