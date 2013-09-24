#!/bin/bash
#
#       /etc/rc.d/init.d/puliserver
#
#       Serveur de dispatch puli
#       <any general comments about this init script>
#
# pidfile:/var/run/dispatcher.pid
# chkconfig: 345 70 70

export PYTHONPATH=/opt/puli

CMD="python2.6 /opt/puli/scripts/dispatcherd.py -C -d"
SERVICE="puliserver"
PIDFILE="/var/run/dispatcher.pid"

# Source function library.
. /etc/init.d/functions

start() {
        echo -n "Starting ${SERVICE}: "
        daemon --pidfile ${PIDFILE} ${CMD}
        echo -e "\n"
        touch /var/lock/subsys/${SERVICE}
        return 0
}

stop() {
        echo -n "Shutting down ${SERVICE}: "
        killproc -p ${PIDFILE} python 
        echo -e "\n"
        rm -f /var/lock/subsys/${SERVICE}
        return 0
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        echo -en "Status of ${SERVICE}: \n"
        status -p ${PIDFILE} python
        ;;
    restart)
        stop
        start
        ;;
    *)
        echo "Usage: ${SERVICE} {start|stop|status}"
        exit 1
        ;;
esac
exit $?
