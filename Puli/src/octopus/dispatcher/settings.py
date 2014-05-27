#!/usr/bin/python2.6
# -*- coding: utf8 -*-

"""
Module holding core value for the dispatcher process.
These values might be overriden by user arguments and should not be reloaded.
Value which could be reloaded during execution (using "reconfig" webservice) should be defined in "settings.py".
"""
__author__      = "Arnaud Chassagne"
__copyright__   = "Copyright 2010, Mikros Image"


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

VERSION = "1.6.10"

DEBUG = False

PORT = 8004
ADDRESS = getLocalAddress()
RUN_AS = None


#
# DEV/EVAL/PROD ENV
#

# EVAL ENV
#BASEDIR = "/s/apps/lin/vfx_test_apps/"

# DEV ENV
BASEDIR = "/datas/jsa/"

LOGDIR = BASEDIR + "OpenRenderManagement/logs"
CONFDIR = BASEDIR + "OpenRenderManagement/Puli/etc/puli"

# PROD
#LOGDIR = "/opt/puli/logs"
#CONFDIR = "/opt/puli/conf"

PIDFILE = "/var/run/puli/dispatcher.pid"  # service control pid file


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

DB_URL = "mysql://puliuser:0ct0pus@127.0.0.1/pulidb"
#DB_URL = "sqlite:///path/to/my/database/file.db"


# RN_TIMEOUT = 1200
