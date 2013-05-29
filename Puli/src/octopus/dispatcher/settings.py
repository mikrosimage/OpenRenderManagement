import os


def getLocalAddress():
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('0.0.0.0', 56))
        return s.getsockname()[0]
    except:
        return "127.0.0.1"


def loadSettingsFile(filename):
    g = {}
    l = {}
    execfile(filename, g, l)
    settings = ((name, value) for name, value in l.items() if name.isupper() and  name in globals())
    globals().update(settings)


## SETTINGS ###########################################################

DEBUG = False

PORT = 8004
ADDRESS = getLocalAddress()
RUN_AS = None
#LOGDIR = "/opt/puli/logs"
LOGDIR = "/tmp/puli/logs"
#CONFDIR = "/opt/puli/conf"
CONFDIR = "/Users/red/workspace3.6/Puli/Puli/etc/puli"
PIDFILE = "dispatcher.pid"

RENDERNODE_REQUEST_MAX_RETRY_COUNT = 10
RENDERNODE_REQUEST_DELAY_AFTER_REQUEST_FAILURE = .1

POOLS_BACKEND_TYPE = "db"
#POOLS_BACKEND_TYPE = "file"
#POOLS_BACKEND_TYPE = "ws"

FILE_BACKEND_RN_PATH = os.path.join(CONFDIR, "workers.lst")
FILE_BACKEND_LICENCES_PATH = os.path.join(CONFDIR, "licences.lst")
FILE_BACKEND_POOL_PATH = os.path.join(CONFDIR, "pools")
WS_BACKEND_URL = "dev.hd3d.fr"
WS_BACKEND_PORT = 11800

DB_ENABLE = True
DB_CLEAN_DATA = False

DB_URL = "mysql://red:0ct0pus@127.0.0.1/pulidb"
#DB_URL = "mysql://puliuser:0ct0pus@127.0.0.1/pulidb"
#DB_URL = "sqlite:///path/to/my/database/file.db"

RN_TIMEOUT = 1200.0
