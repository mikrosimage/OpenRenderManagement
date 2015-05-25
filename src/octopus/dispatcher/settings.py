#!/usr/bin/python
# -*- coding: utf8 -*-

"""
Module holding core value for the dispatcher process.
These values might be overriden by user arguments and should not be reloaded.
Value which could be reloaded during execution (using "reconfig" webservice)
should be defined in "settings.py".
"""
__author__ = "Arnaud Chassagne"
__copyright__ = "Copyright 2010, Mikros Image"


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
    settings = ((name, value) for name, value in l.items() if name.isupper() and name in globals())
    globals().update(settings)


## SETTINGS ###########################################################

VERSION = os.environ.get("REZ_PULI_VERSION", '-')

DEBUG = False

PORT = 8004
ADDRESS = getLocalAddress()
RUN_AS = None


#
# LOG AND CONF FOLDERS
#
LOGDIR = "__LOGDIR_PLACEHOLDER__"
CONFDIR = "__CONFDIR_PLACEHOLDER__"

#
# COMMAND LAUNCHED DURING SHUTDOWN TO RESTART SERVER
#
RESTART_COMMAND = "sudo systemctl restart puliserver.service"

#
# SERVICE CONTROL PID FILE
#
PIDFILE = "/var/run/puli/dispatcher.pid"


#
# PERSISTENCE MECANISM
#
POOLS_BACKEND_TYPE = "db"
#POOLS_BACKEND_TYPE = "file"
#POOLS_BACKEND_TYPE = "ws"

FILE_BACKEND_RN_PATH = os.path.join(CONFDIR, "workers.lst")
FILE_BACKEND_LICENCES_PATH = os.path.join(CONFDIR, "licences.lst")
FILE_BACKEND_POOL_PATH = os.path.join(CONFDIR, "pools")
WS_BACKEND_URL = ""
WS_BACKEND_PORT = 11800

DB_ENABLE = True
DB_CLEAN_DATA = False

DB_URL = "__DB_URL_PLACEHOLDER__"
STAT_DB_URL = "__STAT_DB_URL_PLACEHOLDER__"
