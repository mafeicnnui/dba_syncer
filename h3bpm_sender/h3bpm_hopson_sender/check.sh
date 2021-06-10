#!/usr/bin/sh
export PYTHON3_HOME=/home/hopson/apps/usr/webserver/python3.6.0
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
export RUN_HOME="/home/hopson/apps/usr/webserver/h3bpm_hopson_sender"
#cd ${RUN_HOME}
i_counter=`ps -ef | grep h3bpm_hopson_sender | grep h3bpm_hopson_sender.py  | grep -v grep | grep -v vi | wc -l`

if [ "${i_counter}" -eq "0" ]; then 
   echo 'starting h3bpm_hopson_sender...'
   nohup ${PYTHON3_HOME}/bin/python3 ${RUN_HOME}/h3bpm_hopson_sender.py -conf ${RUN_HOME}/h3bpm_hopson_sender.ini &>>${RUN_HOME}/h3bpm_hopson_sender.log  &
   echo 'starting h3bpm_hopson_sender...success!'
fi
