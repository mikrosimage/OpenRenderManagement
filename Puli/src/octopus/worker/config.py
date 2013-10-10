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
# COMUNICATION BEHAVIOUR
# 
WORKER_SYSINFO_DELAY = 10                          # interval between 2 heartbeats to the server
WORKER_REGISTER_DELAY_AFTER_FAILURE = 15           # wait 15s before retrying to register to the server

WORKER_REQUEST_MAX_RETRY_COUNT = 10                # nb of retry for a failed request
WORKER_REQUEST_DELAY_AFTER_REQUEST_FAILURE = .5    # wait 500ms before resending a request in case of failure (each retry will have a 2 x longer delay)

