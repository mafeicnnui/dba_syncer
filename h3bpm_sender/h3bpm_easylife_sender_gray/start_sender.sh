export PYTHON3_HOME=/home/hopson/apps/usr/webserver/python3.6.0
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
export SYNC_HOME=/home/hopson/apps/usr/webserver/h3bpm_easylife_sender_gray
${PYTHON3_HOME}/bin/python3 ${SYNC_HOME}/h3bpm_easylife_sender.py -conf ${SYNC_HOME}/h3bpm_easylife_sender.ini
