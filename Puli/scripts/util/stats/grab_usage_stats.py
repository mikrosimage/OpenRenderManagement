#!/usr/bin/python2.6
# -*- coding: utf8 -*-

"""
"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2014, Mikros Image"

import os
import sys
import csv
import logging
import datetime
from optparse import OptionParser



try:
    import simplejson as json
except ImportError:
    import json

from tornado.httpclient import HTTPClient, HTTPError
from octopus.dispatcher import settings

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

    _request = "http://%s:%s/stats" % ( options.hostname, options.port )

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
                with open("/".join([settings.LOGDIR,"usage_stats.log"]), "a") as logFile:
                    logFile.write( json.dumps(response.body)+",\n" )

    except HTTPError as e:
        print "Error:", e
    
    del(http_client)
