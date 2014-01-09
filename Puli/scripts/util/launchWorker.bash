#!/bin/bash

PULIHOST=$1
PORT=$2

pidfile=/tmp/worker$PORT.pid
killfile=/tmp/render/kill$PORT

echo "Starting worker: `hostname -f`:$PORT on $PULIHOST"

export PYTHONPATH=/s/apps/lin/eval/puli/distrib/OpenRenderManagement/Puli/src:${PYTHONPATH}
/s/apps/lin/eval/puli/distrib/OpenRenderManagement/Puli/scripts/workerd.py -s $PULIHOST -p $PORT -C -D -P $pidfile -K $killfile
