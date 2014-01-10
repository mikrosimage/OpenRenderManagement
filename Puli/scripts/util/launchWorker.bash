#!/bin/bash

PULIHOST=$1
WORKERPORT=$2

pidfile=/tmp/worker$WORKERPORT.pid
killfile=/tmp/render/kill$WORKERPORT

echo "Starting worker: `hostname -f`:$WORKERPORT on $PULIHOST"

export PYTHONPATH=/s/apps/lin/eval/puli/distrib/OpenRenderManagement/Puli/src:${PYTHONPATH}
/s/apps/lin/eval/puli/distrib/OpenRenderManagement/Puli/scripts/workerd.py -s $PULIHOST -p $WORKERPORT -C -D -P $pidfile -K $killfile
