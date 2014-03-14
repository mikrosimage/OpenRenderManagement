#!/bin/bash
#
# Install startup script on local host.
# To be launched with root user
#


# Useful colored print function
function echo_blue {
    BLUE='\033[1;34m'
    NORMAL='\033[0;39m'

    echo -e $BLUE"$*"$NORMAL
}


# Folder containing new script version
#SOURCE_FOLDER=/s/apps/lin/vfx_test_apps/OpenRenderManagement/Puli/scripts/startup
SOURCE_FOLDER=/s/apps/lin/puli/scripts/startup

# Where to place pid files for the puli services
PID_DIR=/var/run/puli

echo ""
echo_blue "1/6 Interrupt running commands"
/s/apps/lin/bin/mylawn -k


echo ""
echo_blue "2/6 Prepare PID dir"

echo "     - Add directive to create PID dir on boot in /etc/tmpfiles.d/puli.conf"
echo "d /var/run/puli 755 root root - -" > /etc/tmpfiles.d/puli.conf

if [ ! -d "$PID_DIR" ]
then
    echo "     - Create dir: $PID_dIR"
    mkdir /var/run/puli
else
    echo "     - pid dir already exists: $PID_dIR"    
fi

echo "     - Give ownership to user \"render\""
chown -R render /var/run/puli

echo ""
echo_blue "3/6 Install startup scripts"

echo "     - Copy env declaration to /etc/sysconfig"
rsync -L ${SOURCE_FOLDER}/pulirespawner ${SOURCE_FOLDER}/puliworker /etc/sysconfig

echo "     - Copy service declaration to /lib/systemd/system/"
rsync -L ${SOURCE_FOLDER}/pulirespawner.service ${SOURCE_FOLDER}/puliworker.service /lib/systemd/system/

echo ""
echo_blue "4/6 Kill running service"
echo "     - Try to stop init.d services and remove corresponding script"
if [ -f "/etc/init.d/puliworker" ] ; then
    /etc/init.d/puliworker stop
    rm -v /etc/init.d/puliworker
fi

if [ -f "/etc/init.d/pulirespawner" ] ; then
    /etc/init.d/pulirespawner stop
    rm -v /etc/init.d/pulirespawner
fi

echo "     - Check exisiting processes \"respawner\" and \"workerd.py\""
RESPAWN_PID=$(ps aux | grep respawner | grep -v grep | awk '{print $2}' | xargs)
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
    kill -9 ${RESPAWN_PID} ${WORKER_PID}
else
    echo "       No process found" 

    # FIX: This is done to allow a user using cssh to enter user input globally.
    read -p "       Continue (press a key and return) ? " 
fi

echo ""
echo_blue "5/6 Start new services"
systemctl --system daemon-reload
systemctl enable puliworker.service
systemctl enable pulirespawner.service
systemctl start puliworker.service
systemctl start pulirespawner.service
sleep 1s

echo ""
echo_blue "6/6 Check status"
systemctl status puliworker.service
systemctl status pulirespawner.service

echo_blue "Done."
