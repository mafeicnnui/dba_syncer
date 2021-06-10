#!/usr/bin/sh
export PYTHON3_HOME=/home/zqlx/apps/usr/webserver/python3.6.0
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
export RUN_HOME="/home/zqlx/apps/usr/webserver/h3bpm_easylife_sender"
#cd ${RUN_HOME}
i_counter=`ps -ef | grep h3bpm_easylife_sender | grep h3bpm_easylife_sender.py  | grep -v grep | grep -v vi | wc -l`

if [ "${i_counter}" -eq "0" ]; then 
   echo 'starting h3bpm_easylife_sender...'
   #nohup ${PYTHON3_HOME}/bin/python3 ${RUN_HOME}/h3bpm_easylife_sender.py -conf ${RUN_HOME}/h3bpm_easylife_sender.ini &
   ${PYTHON3_HOME}/bin/python3 ${RUN_HOME}/h3bpm_easylife_sender.py -conf ${RUN_HOME}/h3bpm_easylife_sender.ini &>>${RUN_HOME}/h3bpm_easylife_sender.log 
   echo 'starting h3bpm_easylife_sender...success!'
fi
