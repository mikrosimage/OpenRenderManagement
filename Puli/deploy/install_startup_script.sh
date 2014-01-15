#!/bin/bash
#
# Install startup script on local host.
#


# Useful colored print function
function echo_blue {
    BLUE='\033[1;34m'
    NORMAL='\033[0;39m'

    echo -e $BLUE"$*"$NORMAL
}


# Folder containing new script version
SOURCE_FOLDER=/s/apps/lin/eval/puli/distrib/OpenRenderManagement/Puli/scripts/startup

# Where to place pid files for the puli services
PID_DIR=/var/run/puli

echo ""
echo_blue "1/5 Prepare PID dir"

if [ ! -d "$PID_DIR" ]
then
    echo "     - Create dir: $PID_dIR"
    sudo mkdir /var/run/puli
else
    echo "     - pid dir already exists: $PID_dIR"    
fi

echo "     - Give ownership to user \"render\""
sudo chown -R render /var/run/puli

echo ""
echo_blue "2/5 Install startup scripts"
echo "     - Copy env declaration to /etc/sysconfig"
sudo rsync -uL ${SOURCE_FOLDER}/pulirespawner ${SOURCE_FOLDER}/puliworker /etc/sysconfig

echo "     - Copy service declaration to /lib/systemd/system/"
sudo rsync -uL ${SOURCE_FOLDER}/pulirespawner.service ${SOURCE_FOLDER}/puliworker.service /lib/systemd/system/

echo ""
echo_blue "3/5 Kill running service"
echo "     - Try to stop init.d services and remove corresponding script"
if [ -f "/etc/init.d/puliworker" ] ; then
    sudo /etc/init.d/puliworker stop
    sudo rm -v /etc/init.d/puliworker
fi

if [ -f "/etc/init.d/pulirespawner" ] ; then
    sudo /etc/init.d/pulirespawner stop
    sudo rm -v /etc/init.d/pulirespawner
fi

echo "     - Check exisiting processes \"respawner.py\" and \"workerd.py\""
RESPAWN_PID=$(ps aux | grep respawnerd.py | grep -v grep | awk '{print $2}' | xargs)
WORKER_PID=$(ps aux | grep workerd.py | grep -v grep | awk '{print $2}' | xargs)

# Check existing services
if [[ ! -z "$RESPAWN_PID" ]] || [[ ! -z "$WORKER_PID" ]]
then
    echo "       Warning: one or several processes for respawner and worker daemon are still running:"
    echo "       ....respawner: ${RESPAWN_PID}"
    echo "       .......worker: ${WORKER_PID}"
    read -p "       We need to interrupt them to finish install, continue (y/n) ? "

    if [[ "$REPLY" != "y" ]]
    then
      echo "Installation interrupted by user." 
      exit 1
    fi
    sudo kill -9 ${RESPAWN_PID} ${WORKER_PID}
else
    echo "       No process found" 
fi

echo ""
echo_blue "4/5 Start new services"
sudo systemctl --system daemon-reload
sudo systemctl start puliworker.service
sudo systemctl start pulirespawner.service
sleep 1s

echo ""
echo_blue "5/5 Check status"
sudo systemctl status puliworker.service
sudo systemctl status pulirespawner.service

echo_blue "Done."
