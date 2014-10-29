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
import time
import logging
import copy

from itertools import groupby, count

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
# {
#     "prod":{
#           "ddd" :     { "jobs":15, "err":1, "paused":2, "ready/blocked":10, "running":2, "allocatedRN":5, "readyCommandCount":15},
#           "dior_tea" :    { "jobs":1, "err":0, "paused":0, "ready/blocked":0, "running":1, "allocatedRN":1, "readyCommandCount":15},
#     }, 
#     "pool":{
#           "DDD" :     { "jobs":15, "err":1, "paused":2, "ready/blocked":10, "running":2 , "allocatedRN":5, "readyCommandCount":15},
#           "DDD_katana" :  { "jobs":1, "err":0, "paused":0, "ready/blocked":0, "running":1 , "allocatedRN":1, "readyCommandCount":15},
#           "DDD_gpu" : { "jobs":1, "err":0, "paused":0, "ready/blocked":0, "running":1 , "allocatedRN":1, "readyCommandCount":15}, 
#     }, 
#     "user":{
#           "brr" :     { "jobs":15, "err":1, "paused":2, "ready/blocked":10, "running":2 , "allocatedRN":5, "readyCommandCount":15},
#           "bho" :     { "jobs":1, "err":0, "paused":0, "ready/blocked":0, "running":1 , "allocatedRN":1, "readyCommandCount":15},
#           "lap" :     { "jobs":1, "err":0, "paused":0, "ready/blocked":0, "running":1 , "allocatedRN":1, "readyCommandCount":15}, 
#     }, 
#     "step":{
#     ...
#     }, 
#     "type":{
#     ...
#     }, 
#     "total": { "jobs":15, "err":1, "paused":2, "ready/blocked":10, "running":2 , "allocatedRN":5, "readyCommandCount":150}
#     "requestDate": "Wed Apr  2 12:16:01 2014"
# }



def process_args():
    '''
    Manages arguments parsing definition and help information
    '''

    usage = ""
    desc="""Retrieves queue info of the server (http://puliserver:8004/query) and append it in the usage_stats.log file.
This is generally used in a cron script to grab queue usage data over time. It can then be processed to generate several graphs."""

    parser = OptionParser(usage=usage, description=desc, version="%prog 0.1" )
    parser.add_option( "-v", action="store_true", dest="verbose", help="Verbose output" )
    parser.add_option( "-s", "--server", action="store", dest="hostname", default="pulitest", help="Specified a target host to send the request")
    parser.add_option( "-p", "--port", action="store", dest="port", type="int", default=8004, help="Specified a target port")
    parser.add_option( "-o", action="store", dest="outputFile", default=os.path.join(settings.LOGDIR, "queue_stats.log"), help="Target output file." )
    options, args = parser.parse_args()

    return options, args

def aggregate( field, jobs ):
    res = {}
    jobs = sorted( jobs, key=lambda n: n[ field ] )

    for group, iterJobs in groupby(jobs, key= lambda n: n[field]) :
        numBlock, numReady, numRun, numDone, numErr, numCancel, numPaused, numCommands, numAlloc, numJobs = 0,0,0,0,0,0,0,0,0,0
        for job in list(iterJobs):
            # print job
            if job['status']==0:
                numBlock += 1
            elif job['status']==1:
                numReady += 1 
            elif job['status']==2:
                numRun += 1
            elif job['status']==3:
                numDone += 1
            elif job['status']==4:
                numErr += 1
            elif job['status']==5:
                numCancel += 1
            elif job['status']==6:
                numPaused += 1

            numCommands += job['readyCommandCount']
            numAlloc += job['allocatedRN']
            numJobs += 1

        res[group] = { "jobs": numJobs, "err":numErr, "paused":numPaused, "ready":(numReady+numBlock), "running":numRun, "allocatedRN":numAlloc, "readyCommandCount": numCommands }
    
    return res

def formatData( data ):
    # Init result dict    
    res = { "prod":{}, "user":{}, "step":{}, "type":{}, "total":{} }
    jobs = data['items']

    res['requestDate'] = time.mktime(time.strptime(data['summary']['requestDate']))
    res['prod'] = aggregate( 'prod', jobs )
    res['step'] = aggregate( 'step', jobs )
    res['type'] = aggregate( 'type', jobs )
    res['user'] = aggregate( 'user', jobs )
    
    #
    # Get totals (slightly different from other aggregate
    #
    jobs = sorted( jobs, key=lambda n: n['status'] )
    res['total'] = { 'jobs':len(jobs), "err":0, "paused":0, "ready":0, "running":0, "allocatedRN":0, "readyCommandCount": 0 }

    for group, iterJobs in groupby(jobs, key= lambda n: n['status']) :
        jobsBygroup = list(iterJobs)
        if group in [0, 1]:
            res['total']['ready'] += len(jobsBygroup)
        elif group == 2:
            res['total']['running'] = len(jobsBygroup)
        elif group == 4:
            res['total']['err'] = len(jobsBygroup)
        elif group == 6:
            res['total']['paused'] = len(jobsBygroup)
        
        for job in jobsBygroup:
            res['total']['allocatedRN'] += job['allocatedRN']
            res['total']['readyCommandCount'] += job['readyCommandCount']

    return res


if __name__ == "__main__":

    options, args = process_args()
    singletonconfig.load( settings.CONFDIR + "/config.ini" )
    
    # 
    # Prepare request and store result in log file
    #
    _param = "query?&constraint_status=0&constraint_status=1&constraint_status=2&constraint_status=4\
&constraint_status=6\
&attr=id\
&attr=status\
&attr=allocatedRN\
&attr=readyCommandCount\
&attr=user\
&attr=tags%3Aprod\
&attr=tags%3Astep\
&attr=tags%3Ashot\
&attr=tags%3Atype"


    _request = "http://%s:%s/%s" % ( options.hostname, options.port, _param)
    _logPath = os.path.join( options.outputFile )

    if options.verbose:
        print "Command options: %s" % options
        print "Command arguments: %s" % args
        print "Query: %s"+_request

    # fileHandler = logging.handlers.RotatingFileHandler( _logPath,
    #                                                     maxBytes=20000000,
    #                                                     backupCount=1,
    #                                                     encoding="UTF-8")

    fileHandler = logging.FileHandler(_logPath, encoding="UTF-8")
    fileHandler.setFormatter(logging.Formatter('%(message)s'))

    statsLogger = logging.getLogger('stats')
    statsLogger.addHandler(fileHandler)
    statsLogger.setLevel(singletonconfig.get('CORE', 'LOG_LEVEL'))

    http_client = HTTPClient()
    try:
        response = http_client.fetch(_request)

        if response.error:
            print "Error:   %s" % response.error
            print "         %s" % response.body
        else:
            if response.body == "":
                print "Error: No stats retrieved"
            else:
                data = json.loads(response.body)

                aggregatedData = formatData(data)
                statsLogger.warning(json.dumps(aggregatedData))

    except HTTPError, e:
        print "Error:", e

    del(http_client)
