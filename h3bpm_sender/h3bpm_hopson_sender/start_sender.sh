export PYTHON3_HOME=/home/hopson/apps/usr/webserver/python3.6.0
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
export SYNC_HOME=/home/hopson/apps/usr/webserver/h3bpm_hopson_sender
${PYTHON3_HOME}/bin/python3 ${SYNC_HOME}/h3bpm_hopson_sender.py -conf ${SYNC_HOME}/h3bpm_sender.ini
