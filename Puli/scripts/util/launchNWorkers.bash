#!/bin/bash

RANGE_START=$1
RANGE_END=$2

echo "Starting worker: `hostname -f`:$PORT on $PULIHOST"

for i in $(seq $RANGE_START $RANGE_END)
do
    #echo "xterm -fa 'Verdana Mono' -fs 8 -geom 150x10 -e \"/s/apps/lin/eval/puli/distrib/OpenRenderManagement/Puli/scripts/util/launchWorker.bash pulitest $i\" &"
    xterm \
        -fa 'Verdana Mono' -fs 8 \
        -geom 150x10 \
        -e "/s/apps/lin/eval/puli/distrib/OpenRenderManagement/Puli/scripts/util/launchWorker.bash pulitest $i; csh -i" &
    # xterm -fa 'Verdana Mono' -fs 8 -geom 150x10 -e "bash -c '/s/apps/lin/eval/puli/distrib/OpenRenderManagement/Puli/scripts/util/launchWorker.bash pulitest '"$i"';sh -i'"
    sleep 0.5s
done
