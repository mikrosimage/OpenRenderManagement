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

import pymongo
from pymongo import MongoClient

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

# {
#     "ts" : ISODate()
#     { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
#     { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
#     { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
#     { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
#     { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
#     { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
# }

# {
#     "ts" : ISODate(),
#     # "day_of_week": x,
#     # "day_of_year": x,
#     # "week_of_year": x,
#     "facts": [
#         { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
#         { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
#         { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
#         { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
#         { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
#         { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
#         { "prod" : "ddd", "pool":"", "type":"", "step":"", "status":"", "user":"" }
#     ]
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
    parser.add_option( "-s", "--server", action="store", dest="hostname", default="puliserver", help="Specified a target host to send the request")
    parser.add_option( "-p", "--port", action="store", dest="port", type="int", default=8004, help="Specified a target port")
    parser.add_option( "-o", action="store", dest="outputFile", default=os.path.join(settings.LOGDIR, "queue_stats.log"), help="Target output file." )
    options, args = parser.parse_args()

    return options, args

# def aggregate( field, jobs ):
#     res = {}
#     jobs = sorted( jobs, key=lambda n: n[ field ] )

#     for group, iterJobs in groupby(jobs, key= lambda n: n[field]) :
#         numBlock, numReady, numRun, numDone, numErr, numCancel, numPaused, numCommands, numAlloc, numJobs = 0,0,0,0,0,0,0,0,0,0
#         for job in list(iterJobs):
#             # print job
#             if job['status']==0:
#                 numBlock += 1
#             elif job['status']==1:
#                 numReady += 1 
#             elif job['status']==2:
#                 numRun += 1
#             elif job['status']==3:
#                 numDone += 1
#             elif job['status']==4:
#                 numErr += 1
#             elif job['status']==5:
#                 numCancel += 1
#             elif job['status']==6:
#                 numPaused += 1

#             numCommands += job['readyCommandCount']
#             numAlloc += job['allocatedRN']
#             numJobs += 1

#         res[group] = { "jobs": numJobs, "err":numErr, "paused":numPaused, "ready":(numReady+numBlock), "running":numRun, "allocatedRN":numAlloc, "readyCommandCount": numCommands }
    
#     return res


def formatData( data ):
    # Init result dict
    res = {}
    jobs = data['items']

    if len(jobs) == 0:
        return None

    res['ts'] = time.mktime(time.strptime(data['summary']['requestDate']))
    res['date'] = datetime.datetime.fromtimestamp( time.mktime(time.strptime(data['summary']['requestDate'])) )
    res['num_facts'] = data['summary']['count']
    res['facts'] = []

    for job in jobs:
        fact = {}
        fact['allocatedRN'] = job['allocatedRN']
        fact['name'] = job['name']
        fact['pool'] = job['pool']
        fact['status'] = job['status']
        fact['user'] = job['user']

        tags = {
                    "prod" : job['prod'], 
                    "shot" : job['shot'],
                    "step" : job['step'], 
                    "type" : job['type'],     
                }
        fact['tags'] = tags

        res['facts'].append( fact )

    return res


if __name__ == "__main__":



    # event =  {
    #             "ts" : datetime.datetime.now(),
    #             "num_facts": 1
    #             "facts": [ 
    #                         { 
    #                             "allocatedRN":3,
    #                             "name":"[Katana|OCC2] s0040_p0070_confo_render_v004",
    #                             "pool":"DDD_katana", 
    #                             "status":"running", 
    #                             "user":"jsa",

    #                             "tags": { 
    #                                 "prod":"ddd", 
    #                                 "shot":"p1",
    #                                 "step":"prelit", 
    #                                 "type":"katana",     
    #                                 },

    #                             # "maxRN":3
    #                             # "dispatchKey":10
    #                             # "commandCount":279
    #                             # "readyCommandCount":268
    #                             # "doneCommandCount":3
    #                         },

    #                     ]
    #         }
    # Soit pour la collection:
    # { ts: xxx, num_facts: n, facts: [ {name:job1, tags:{prod:ddd}}, {name:job2, tags:{prod:ddd}}, {name:job3, tags:{prod:ddd}} ]
    # { ts: xxx, num_facts: n, facts: [ {name:job1, tags:{prod:ddd}}, {name:job2, tags:{prod:ddd}}, {name:job3, tags:{prod:ddd}} ]
    # ...
    # { ts: xxx, num_facts: n, facts: [ {name:job1, tags:{prod:ddd}}, {name:job2, tags:{prod:ddd}}, {name:job3, tags:{prod:ddd}} ]

    # Requetes

    # db.event_queue.find( { facts: { $elemMatch:  {status:1}}} , {ts:1, _id:0, "facts.name":1} )

    # afficher les champs ts pour les documents compris entre 2 TS en triant par TS croissant
    # db.event_queue.find( { ts : {$gt:1401273061,$lte:1401273241} }, {ts:1} ).sort({ts:1})
    
    # faire la sum des num_facts de tous les documents
    # db.event_queue.aggregate( [ { $group:{ _id:null, nbfacts: {$sum: "$num_facts"} } } ] )

    # deplie tous les jobs par ts et fais un group par "minute" du TS (en fait ne fonctionne pas)
    # db.event_queue.aggregate( [ { $unwind : "$facts"}, { $project : { ts_minute : { $minute : "$ts" } } },  { $group : { _id: {ts_minute:"$ts_minute"}, nbfacts_minute: {$sum:1}}}, { $sort : { "_id.ts_minute": 1}}  ] )

    #
    # db.event_queue.aggregate( [ { $unwind : "$facts"}, { $project : { ts_day : { $dayOfYear : "$date" } } },  { $group : { _id: {ts_day:"$ts_day"}, nbfacts_day: {$sum:1}}}, { $sort : { "_id.ts_day": 1}}  ] )

    # import pudb;pu.db
    options, args = process_args()
    singletonconfig.load( settings.CONFDIR + "/config.ini" )
    
    # 
    # Prepare request and store result in log file
    #
    _param = "query?&constraint_status=0&constraint_status=1&constraint_status=2&constraint_status=4\
&constraint_status=6\
&attr=allocatedRN\
&attr=id\
&attr=name\
&attr=pool\
&attr=status\
&attr=user\
&attr=tags%3Aprod\
&attr=tags%3Ashot\
&attr=tags%3Astep\
&attr=tags%3Atype"


    _request = "http://%s:%s/%s" % ( options.hostname, options.port, _param)
    _logPath = os.path.join( options.outputFile )

    if options.verbose:
        print "Command options: %s" % options
        print "Command arguments: %s" % args
        print "Query: %s"+_request


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
                data = json.loads(response.body)

                if options.verbose:
                    print "Data retrieved: %s" % data

                dbData = formatData( data )
                if dbData is not None:
                    try:
                        client = MongoClient()
                        # "mongodb://<user>:<password>@kahana.mongohq.com:10049/pulistats"
                        db = client.pulistats
                        queue = db.event_queue

                        res = queue.insert( dbData )
                        if options.verbose:
                            print "Event properly logged: %s" % res
                    except Exception, e:
                        print "DB Error:", e
                else:
                    if options.verbose:
                        print "No jobs in list, not necessary to add event data in stats"


                # statsLogger.warning( json.dumps(aggregatedData) )

    except HTTPError, e:
        print "Error:", e
   
    del(http_client)

