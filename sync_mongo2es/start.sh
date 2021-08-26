#!/usr/bin/env bash
source ~/.bash_profile
export SCRIPT_PATH=/home/hopson/apps/usr/webserver/dba/script/sync_mongo2es
python3 $SCRIPT_PATH/sync_mongo2es.py -conf $SCRIPT_PATH/sync_mongo2es.ini
