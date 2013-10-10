import socket


def getLocalAddress():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('1.2.3.4', 56))
    return s.getsockname()[0]


def loadSettingsFile(filename):
    g = {}
    l = {}
    execfile(filename, g, l)
    settings = [(name, value) for name, value in l.items() if name.isupper() and name in globals()]
    globals().update(settings)


fqdn = socket.getfqdn(socket.gethostname())
if "." in fqdn:
    computername, domain = fqdn.split(".", 1)
else:
    computername = fqdn


## SETTINGS ###########################################################
## These settings cannot be reloaded during worker execution because some of the values can be overriden with program user arguments
## To change these values, the worker must be restarted

DEBUG = True
computername = computername.lower()

#
# Infos du webservice local
#
PORT = 8000
ADDRESS = computername

#
# Infos du webservice server
#
DISPATCHER_PORT = 8004
DISPATCHER_ADDRESS = "puliserver"

#
# Definition des fichiers de surveillance
#
PIDFILE = "/tmp/worker.pid"
KILLFILE = "/tmp/render/killfile"

RUN_AS = ""
LOGDIR = "/var/log/puli"

LIMIT_OPEN_FILES = 32768

