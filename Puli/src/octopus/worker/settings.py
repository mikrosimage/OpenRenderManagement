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

## SETTINGS ###########################################################

DEBUG = True

PORT = 8000
fqdn = socket.getfqdn(socket.gethostname())
if "." in fqdn:
    computername, domain = fqdn.split(".", 1)
else:
    computername = fqdn
computername = computername.lower()
ADDRESS = computername

PIDFILE = "/tmp/worker.pid"
KILLFILE = "/tmp/render/killfile"

DISPATCHER_PORT = 8004
DISPATCHER_ADDRESS = "puliserver"

RUN_AS = ""
LOGDIR = "/var/log/puli"

LIMIT_OPEN_FILES = 32768
