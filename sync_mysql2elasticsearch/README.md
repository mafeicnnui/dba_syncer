# 安装驱动
export PYTHON3_HOME=/home/hopson/apps/usr/webserver/dba/python3.6.8
export LD_LIBRARY_PATH=$PYTHON3_HOME/lib
$PYTHON3_HOME/bin/pip3 install elasticsearch==7.14 -i https://pypi.douban.com/simple
$PYTHON3_HOME/bin/pip3 install mysql-replication==0.24 -i https://pypi.douban.com/simple

ES 7.14.2 测试
curl --user elastic:elastic 10.2.39.17:9200/_cat/health?v
curl --user elastic:elastic 10.2.39.17:9200/test/block_users/315282917172711424?pretty


ES 7.14.2 腾讯云测试
https://cloud.tencent.com/document/product/845/19538


from elasticsearch import Elasticsearch
es = Elasticsearch(
["
https://es-0h3xrqrp.public.tencentelasticsearch.com:9200"],
http_auth=('elastic','XXXXXXX'),
sniff_on_start=False,
sniff_on_connection_fail=False,
sniffer_timeout=None)
res = es.index(index="my_indextest", doc_type="_doc", id=1, body={"title": "One", "tags":
["ruby"]})
print(res)
res = es.get(index="my_indextest", doc_type="_doc", id=1)
print(res
['_source'])

curl --user elastic:21block@2022 https://es-4nokrpqb.public.tencentelasticsearch.com:9200/21block_block_users/_doc/997?pretty
curl --user elastic:21block@2022 https://es-4nokrpqb.public.tencentelasticsearch.com:9200/21block_block_users/_mapping?pretty
curl -XDELETE --user elastic:21block@2022 https://es-4nokrpqb.public.tencentelasticsearch.com:9200/21block_block_users?pretty

-- HY
curl --user elastic:21block@2022 https://es-4nokrpqb.public.tencentelasticsearch.com:9200/21block_cluster_info2/_doc/22?pretty
curl --user elastic:21block@2022 https://es-4nokrpqb.public.tencentelasticsearch.com:9200/21block_cluster_info/_mapping?pretty
curl -XDELETE --user elastic:21block@2022 https://es-4nokrpqb.public.tencentelasticsearch.com:9200/21block_cluster_info?pretty
curl -XDELETE --user elastic:21block@2022 https://es-4nokrpqb.public.tencentelasticsearch.com:9200/21block_cluster_info2?pretty


curl 10.2.39.41:9200/test?pretty
curl 10.2.39.41:9200/test/xs/35?pretty
curl 10.2.39.41:9200/test/xs/_search?pretty
curl 10.2.39.41:9200/test/xs/_mapping?pretty
curl '10.2.39.41:9200/test/xs/_search?pretty' -d '
{
  "query": {
    "bool": {
      "must": [
        {"term":{"data.xh":3}}
      ]
    }
  }
}'

curl '10.2.39.41:9200/test/xs/_search?pretty' -d '
{
  "query": {
    "bool": {
      "must": [
        {"term":{"data.xm":"zhang.san"}}
      ]
    }
  }
}'

curl '10.2.39.41:9200/test/xs/_search?pretty' -d '
{
  "query": {
    "bool": {
      "must": [
        {"match":{"data.xm":"zhang.san"}}
      ]
    }
  }
}'
curl '10.2.39.41:9200/test/xs/_search?pretty' -d '
{
  "query": {
    "bool": {
      "must": [
        {"match":{"after_values.xh":35}}
      ]
    }
  }
}'
    
es = get_ds_es(ES_SETTINGS['host'], ES_SETTINGS['port'])
es = get_ds_es_auth(ES_SETTINGS['host'], ES_SETTINGS['port'],ES_SETTINGS['user'], ES_SETTINGS['passwd'])

pip3 install elasticsearch==7.14.0 -i https://pypi.douban.com/simple
pip3 install  mysql-replication==0.21  -i https://pypi.douban.com/simple
pip3 install  PyMySQL==0.9.2  -i https://pypi.douban.com/simple_
pip3 install -r requirements.txt

D:\apps\python3.6\python.exe mysql2es_sync.py --conf=mysql2es_sync.json
cd /home/hopson/apps/usr/webserver/dba_syncer/sync_mysql2elasticsearch
python3 mysql2es_sync.py --conf=mysql2es_sync.json

curl -XDELETE --user elastic:21block@2022 192.168.3.43:9200/21block_cluster_info2?pretty
curl --user elastic:21block@2022 192.168.3.43:9200/21block_cluster_info2/_doc/22?pretty


"SYNC_SETTINGS"  : {
    "isSync"   : true,
    "level"    : "1",
    "db"       : "block_cluster",
    "table"    : "cluster_info",
    "logfile"  : "sync_mysql2es.log",
    "batch"    : 5,
    "debug"    : "Y"
},

"MYSQL_SETTINGS" : {
    "host"  : "bj-cynosdbmysql-grp-3k142zlc.sql.tencentcdb.com",
    "port"  : 29333,
    "user"  : "root",
    "passwd": "Dev21@block2022"
},

 "ES_SETTINGS": {
        "host": "es-4nokrpqb.public.tencentelasticsearch.com",
        "port": "9200",
        "schema": "https",
        "user": "elastic",
        "passwd": "21block@2022",
        "index": "21block_cluster_info2",
        "batch": 3,
}