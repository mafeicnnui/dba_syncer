# 安装驱动
export PYTHON3_HOME=/home/hopson/apps/usr/webserver/dba/python3.6.8
export LD_LIBRARY_PATH=$PYTHON3_HOME/lib
$PYTHON3_HOME/bin/pip3 install elasticsearch==7.14 -i https://pypi.douban.com/simple
$PYTHON3_HOME/bin/pip3 install mysql-replication==0.24 -i https://pypi.douban.com/simple


pip3 install elasticsearch==7.14.0 -i https://pypi.douban.com/simple
pip3 install  mysql-replication==0.21  -i https://pypi.douban.com/simple
pip3 install  PyMySQL==0.9.2  -i https://pypi.douban.com/simple_
pip3 install -r requirements.txt

D:\apps\python3.6\python.exe mysql2es_sync.py --conf=mysql2es_sync.json
cd /home/hopson/apps/usr/webserver/dba_syncer/sync_mysql2elasticsearch
python3 mysql2es_sync.py --conf=mysql2es_sync.json

kafka-topics --zookeeper test-hadoop-6:2181 --list
kafka-topics --create --zookeeper test-hadoop-6:2181 --replication-factor 2 --partitions 1 --topic hst_source_tdsql_test4
kafka-console-consumer --bootstrap-server test-hadoop-7:9092,test-hadoop-8:9092,test-hadoop-9:9092 --topic hst_source_tdsql_test4
