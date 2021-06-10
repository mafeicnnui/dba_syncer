export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
export PYTHON3_HOME=/home/hopson/apps/usr/webserver/python3.6.0
i_counter=`ps -ef | grep h3bpm_easylife_sender_uat | grep h3bpm_easylife_sender.ini | grep -v grep | grep -v vi | wc -l`

if [ "${i_counter}" -eq "0" ]; then 
   echo 'starting h3bpm_easylife_sender...'
   nohup /home/hopson/apps/usr/webserver/h3bpm_easylife_sender_uat/start_sender.sh &>/dev/null 2>&1 &
   echo 'starting h3bpm_easylife_sender...success!'
fi
