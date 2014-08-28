#!/usr/bin/python2.6
# -*- coding: utf8 -*-

"""
name: config.py

Module holding param values that might be reloaded without restarting the worker
The worker handles a "reconfig" request wich ask the worker applicaiton to reload this class

"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2013, Mikros Image"


#
# PROCESS BEHAVIOUR
# 
LIST_ALLOWED_PROCESSES_WHEN_PAUSING_WORKER = ['python', 'python2.6', 'bash', 'sshd', 'respawnerd', 'workerd']

#
# COMUNICATION BEHAVIOUR
# 
WORKER_SYSINFO_DELAY = 8                           # interval between 2 heartbeats to the server (usually few seconds)
WORKER_MAX_SYSINFO_DELAY = 900                     # interval between 2 full update to the server (usually several minutes)

WORKER_REGISTER_DELAY_AFTER_FAILURE = 15           # wait 15s before retrying to register to the server

WORKER_REQUEST_MAX_RETRY_COUNT = 8                 # nb of retry for a failed request
WORKER_REQUEST_DELAY_AFTER_REQUEST_FAILURE = .5    # wait 500ms before resending a request in case of failure (each retry will have a 2 x longer delay)

#
# Indicate the log file size in bytes and number of file backups --> 2Mo x 10
#
LOG_SIZE = 2097152
LOG_BACKUPS = 2


#
# HACK
#
CHECK_EXISTING_PROCESS=True
