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

fqdn = socket.getfqdn(socket.gethostname())
if "." in fqdn:
    computername, domain = fqdn.split(".", 1)
else:
    computername = fqdn
computername = computername.lower()

PIDFILE = "/tmp/worker.pid"
KILLFILE = "/tmp/render/killfile_test"

# Worker webservice access
PORT = 8000
ADDRESS = computername

# Remote server webservice access
DISPATCHER_PORT = 8004
DISPATCHER_ADDRESS = "localhost"

RUN_AS = ""
LOGDIR = "/var/log/puli"

LIMIT_OPEN_FILES = 32768
