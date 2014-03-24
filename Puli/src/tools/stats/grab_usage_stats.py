#!/usr/bin/python2.6
# -*- coding: utf8 -*-

"""
"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2014, Mikros Image"

import os
import sys
import csv
import datetime
import logging
from logging import handlers
from optparse import OptionParser



try:
    import simplejson as json
except ImportError:
    import json

from tornado.httpclient import HTTPClient, HTTPError
from octopus.dispatcher import settings
from octopus.core import singletonconfig

###########################################################################################################################
# Data example:



def process_args():
    '''
    Manages arguments parsing definition and help information
    '''

    usage = ""
    desc="""Retrieves stats info of the server (http://puliserver:8004/stats) and append it in the usage_stats.log file.
This is generally used in a cron script to grab renderfarm usage data over time. It can then be processed to generate several graphs."""

    parser = OptionParser(usage=usage, description=desc, version="%prog 0.1" )

    parser.add_option("-s", "--server", action="store", dest="hostname", default="pulitest", help="Specified a target host to send the request")
    parser.add_option("-p", "--port", action="store", dest="port", type="int", default=8004, help="Specified a target port")

    options, args = parser.parse_args()

    return options, args


if __name__ == "__main__":

    options, args = process_args()
    singletonconfig.load( settings.CONFDIR + "/config.ini" )
    
    # 
    # Prepare request and store result in log file
    #
    _request = "http://%s:%s/stats" % ( options.hostname, options.port )
    _logPath = os.path.join(settings.LOGDIR, "usage_stats.log")

    # fileHandler = logging.handlers.RotatingFileHandler( _logPath, 
    #                                                     maxBytes=5000, 
    #                                                     backupCount=singletonconfig.get('CORE','LOG_BACKUPS'), 
    #                                                     encoding="UTF-8")
    fileHandler = logging.handlers.RotatingFileHandler( _logPath, 
                                                        maxBytes=singletonconfig.get('CORE','LOG_SIZE'), 
                                                        backupCount=singletonconfig.get('CORE','LOG_BACKUPS'), 
                                                        encoding="UTF-8")


    fileHandler.setFormatter( logging.Formatter('%(message)s') )
    
    statsLogger = logging.getLogger('stats')
    statsLogger.addHandler( fileHandler )
    statsLogger.setLevel( singletonconfig.get('CORE','LOG_LEVEL') )


    statsLogger.debug("test")

    http_client = HTTPClient()
    try:
        response = http_client.fetch( _request )

        if response.error:
            print "Error:   %s" % response.error
            print "         %s" % response.body
        else:
            if response.body == "":
                print "Error: No stats retrieved"
            else:
                statsLogger.warning( response.body )

    except HTTPError as e:
        print "Error:", e
   
    del(http_client)

