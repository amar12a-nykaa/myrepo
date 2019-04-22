#!/bin/bash

cd /tmp
sudo wget https://www.python.org/ftp/python/3.5.2/Python-3.5.2.tgz
sudo tar xzf Python-3.5.2.tgz
cd Python-3.5.2
sudo ./configure
sudo make altinstall
sudo ln -s -f /usr/local/bin/python3.5 /usr/bin/python3
sudo ln -s -f /usr/local/bin/pip3.5 /usr/bin/pip3
sudo yum install -y postgresql-devel

sudo pip3 install psycopg2
sudo pip3 install pandas
sudo pip3 install argparse
sudo pip3 install boto3
sudo pip3 install mysqlclient
sudo pip3 install mysql-connector-python
sudo pip3 install elasticsearch
