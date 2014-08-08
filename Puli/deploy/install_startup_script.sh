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

PAUSE=
# Assign process vars with parameters
while getopts “p” OPTION
do
     case $OPTION in
         p)
             PAUSE=0
             ;;
         ?)
             echo "Error: Unknown option."
             exit
             ;;
     esac
done

# Folder containing new script version
SOURCE_FOLDER=/s/apps/lin/vfx_test_apps/OpenRenderManagement/Puli/scripts/startup

# Where to place pid files for the puli services
PID_DIR=/var/run/puli

echo ""
echo_blue "Interrupt running commands"
/s/apps/lin/utils/mylawn/mylawn -k


echo ""
echo_blue "1/5 Prepare PID dir"

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
echo_blue "2/5 Install startup scripts"

echo "     - Copy env declaration to /etc/sysconfig"
rsync -L ${SOURCE_FOLDER}/puliworker /etc/sysconfig

echo "     - Copy service declaration to /lib/systemd/system/"
rsync -L ${SOURCE_FOLDER}/puliworker.service /lib/systemd/system/

echo ""
echo_blue "3/5 Interrupting services"
echo "     - Try to stop old services and remove corresponding script"
if [ -f "/etc/init.d/puliworker" ] ; then
    /etc/init.d/puliworker stop
    rm -v /etc/init.d/puliworker
fi

if [ -f "/etc/init.d/pulirespawner" ] ; then
    /etc/init.d/pulirespawner stop
    rm -v /etc/init.d/pulirespawner
fi

echo "     - Try to stop new services"
systemctl --system daemon-reload

if [ -f "/lib/systemd/system/pulirespawner.service" ] ; then
    systemctl stop pulirespawner.service
#    rm -v /lib/systemd/system/pulirespawner.service
#    rm -v /etc/sysconfig/pulirespawner
fi

if [ -f "/lib/systemd/system/puliworker.service" ] ; then
    systemctl stop puliworker.service
fi


echo "     - Check exisiting processes \"respawner\" and \"worker\""
RESPAWN_PID=$(ps aux | grep respawner | grep -v grep | awk '{print $2}' | xargs)
WORKER_PID=$(ps aux | grep workerd.py | grep -v grep | awk '{print $2}' | xargs)

# Check existing services
if [[ ! -z "$RESPAWN_PID" ]] || [[ ! -z "$WORKER_PID" ]]
then
    echo "       Warning: one or several processes for respawner daemon are still running:"
    echo "       ....respawner: ${RESPAWN_PID}"
    echo "       ....worker: ${WORKER_PID}"
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
echo_blue "4/5 Enable and restart services"
systemctl --system daemon-reload
systemctl enable puliworker.service
systemctl enable pulirespawner.service
systemctl start puliworker.service
systemctl start pulirespawner.service
sleep 1s

echo ""
echo_blue "5/5 Check status"
systemctl status puliworker.service
systemctl status pulirespawner.service


if [[ -z $PAUSE ]]
then
    echo ""
    echo_blue "Unpause worker"
    /s/apps/lin/utils/mylawn/mylawn 0
else
    echo ""
    echo_blue "Let worker in pause mode"
    /s/apps/lin/utils/mylawn/mylawn 1
fi

echo_blue "Done."
