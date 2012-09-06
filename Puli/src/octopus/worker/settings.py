import socket


def getLocalAddress():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('1.2.3.4', 56))
    return s.getsockname()[0]


def loadSettingsFile(filename):
    g = {}
    l = {}
    execfile(filename, g, l)
    settings = [(name, value) for name, value in l.items() if name.isupper() and  name in globals()]
    globals().update(settings)

## SETTINGS ###########################################################

DEBUG = True

PORT = 8000
computername, domain = socket.getfqdn(socket.gethostname()).split(".", 1)
computername = computername.lower()
ADDRESS = computername

PIDFILE = "/tmp/worker.pid"
KILLFILE = "/tmp/render/killfile"

DISPATCHER_PORT = 8004
DISPATCHER_ADDRESS = "puliserver"

RUN_AS = ""
LOGDIR = "/tmp/puli/logs"
