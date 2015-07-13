#!/usr/bin/python
# -*- coding: utf8 -*-

from __future__ import absolute_import

"""
name: settings.py

Module holding core value for the worker process.
These values might be overriden by user arguments and should not be reloaded.
Value which could be reloaded during execution (using "reconfig" webservice) should be defined in settings.py.
"""
__author__ = "Arnaud Chassagne"
__copyright__ = "Copyright 2010, Mikros Image"

import socket
import os

try:
    # When using rez, "package" module is located outside the src folder.
    # It is only available for import if rez is used to resolve env.
    from package import version as puliversion
except ImportError:
    puliversion = "__PULIVERSION__"

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
VERSION = os.environ.get("REZ_PULI_VERSION", puliversion)

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
PIDFILE = "/var/run/puli/worker.pid"  # service control pid file

KILLFILE = "/datas/tmp/render/killfile%s" % PORT        # indicate if the worker needs to be paused, kill and paused or resumed
RESTARTFILE = "/tmp/render/restartfile"  # indicate if the worker must be restarted by the respawner

RUN_AS = ""
LOGDIR = "/var/log/puli"

LIMIT_OPEN_FILES = 32768

PIDFILES_TO_KEEP=["/var/run/tractor-blade.pid","/var/run/hqueue.pid" ]
