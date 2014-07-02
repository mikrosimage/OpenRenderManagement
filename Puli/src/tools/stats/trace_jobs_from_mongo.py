# -*- coding: utf8 -*-

"""
"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2014, Mikros Image"

import os
import sys
import csv
import time
import datetime
from optparse import OptionParser

import pygal
from pygal.style import *

import pymongo
from pymongo import MongoClient

try:
    import simplejson as json
except ImportError:
    import json


from tools.stats.common import getRangeDates, prepareGraph, renderGraph


def createParser():
    """
    Create a OptionParser object to handle common params for stats commands
    """
    parser = OptionParser()

    # parser.add_option( "-f", action="store", dest="sourceFile", help="Source file" )
    parser.add_option( "-o", action="store", dest="outputFile", help="Target output file." )
    parser.add_option( "--render-mode", action="store", dest="renderMode", type="string", help="render destination: inline, svg or png", default="svg" )

    parser.add_option( "-v", action="store_true", dest="verbose", help="Verbose output" )

    parser.add_option( "-s", action="store", dest="rangeIn", type="int", help="Start range is N hours in past", default=3 )
    parser.add_option( "-e", action="store", dest="rangeOut", type="int", help="End range is N hours in past (mus be lower than '-s option'", default=0 )
    parser.add_option( "--startTime", action="store", dest="timeIn", type="int", help="Start range is at timestamp", default=0 )
    parser.add_option( "--endTime", action="store", dest="timeOut", type="int", help="End range is at timestamp", default=time.time() )

    parser.add_option( "--filter", action="store", dest="filterExpr", type="string", help="Filter expression" )

    parser.add_option( "-t", "--title", action="store", dest="title", help="Indicates a title", default="")
    parser.add_option( "-r", "--res", action="store", dest="resolution", type="int", help="Indicates ", default=60 )

    # parser.add_option( "--scale", action="store", dest="scaleEvery", type="int", help="Indicates the number of scale values to display", default=8 )
    # parser.add_option( "--scaleRound", action="store", dest="scaleRound", type="int", default=3600 )

    parser.add_option( "--stack", action="store_true", dest="stacked", default=False)
    parser.add_option( "--line", action="store_true", dest="line", default=True)
    parser.add_option( "--interpolate", action="store_true", dest="interpolate", default=False )
    parser.add_option( "--log", action="store_true", dest="logarithmic", help="Display graph with a logarithmic scale", default=False )
    parser.add_option( "--style", action="store", dest="style", help="Set a specific style name (BlueStyle, RedBlueStyle ...)", default="RedBlue" )
    parser.add_option( "--scaleRotation", action="store", dest="scaleRotation", type="int", default=45 )
    parser.add_option( "--width", action="store", dest="width", type="int" )
    parser.add_option( "--height", action="store", dest="height", type="int" )
    parser.add_option( "--majorCount", action="store", dest="majorCount", type="int" )
    # parser.add_option( "--hide-x", action="store_false", dest="showX", default=True)
    # parser.add_option( "--hide-y", action="store_false", dest="showY", default=True)
    return parser


def getData(startDate, EndDate, timeInterval, filter, group=None, value=None):
    global prevTime

    client = MongoClient()
    db = client.pulistats
    queue = db.event_queue

    dateIn = datetime.datetime.fromtimestamp(startDate)
    dateOut = datetime.datetime.fromtimestamp(endDate)

    # dateIn = datetime.datetime(2014, 6, 1, 10, 30, 00, 0)
    # dateOut = datetime.datetime(2014, 7, 6, 16, 30, 00, 0)

    print "%s - %6.2f ms - request prepared" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    prevTime = time.time()

    matchDate = { "date" : {"$gt":dateIn, "$lt":dateOut} }
    matchFilter = { "facts.tags.prod": "ddd" }

    # Set custom fields to be retrieved from facts attributes (including job tags)
    fields = {
        "allocatedRN" : "$facts.allocatedRN",
        "name" : "$facts.name",
        "prod" : "$facts.tags.prod",
        "status" : "$facts.status",
        }
    # Add default fields
    fields["ts"]=1
    fields["date"]=1
    fields["_id"]=1

    # groupBytime
    group = { "_id" : { "timeInterval" : { '$subtract' :[ {'$divide' : ['$ts', timeInterval ]}, { '$mod' : [{'$divide' : ['$ts', timeInterval ]},1] } ] } }, }
    group["count_jobs"]={"$sum":1}
    group["count_ready"]={"$sum":{ "$cond": [ {"$ne":["$status",1]},0,1]} }
    group["count_running"]={"$sum":{ "$cond": [ {"$ne":["$status",2]},0,1]} }
    group["count_error"]={"$sum":{ "$cond": [ {"$ne":["$status",4]},0,1]} }
    group["count_paused"]={"$sum":{ "$cond": [ {"$ne":["$status",6]},0,1]} }

    # calculatedFields
    resultFields = { "avg_jobs": {"$divide": ["$count_jobs", (timeInterval/60)] } }
    resultFields["avg_ready"]={"$divide": ["$count_ready", (timeInterval/60)] }
    resultFields["avg_running"]={"$divide": ["$count_running", (timeInterval/60)] }
    resultFields["avg_error"]={"$divide": ["$count_error", (timeInterval/60)] }
    resultFields["avg_paused"]={"$divide": ["$count_paused", (timeInterval/60)] }
    # countFields
    resultFields["count_ready"]=1
    resultFields["count_running"]=1
    resultFields["count_error"]=1
    resultFields["count_paused"]=1
    resultFields["count_jobs"]=1

    request = [
        { "$match": matchDate },
        { "$unwind": "$facts" },
        { "$match": matchFilter },
        { "$project": fields },
        { "$group": group },
        { "$project": resultFields },
        { "$sort": {"_id":1} }
    ]


    print request

    # print json.dumps(request, indent=4)

    res = queue.aggregate( request )
    # res = queue.aggregate([
#             { "$match": { "date" : {"$gt":dateIn, "$lt":dateOut} } },
#             { "$unwind": "$facts"},
#             # { "$match": { "facts.tags.prod": "test" }},
#             { "$project": {
# #                    "allocatedRN" : "$facts.allocatedRN",
#                     "name" : "$facts.name",
#                     "prod" : "$facts.tags.prod",
#                     "status" : "$facts.status",
#                     "ts": 1,
#                     "date": 1,
#                     "_id": 0
#                 }},
#            { "$group": {
#                 "_id" : { "timeInterval" : { '$subtract' :[ {'$divide' : ['$ts', timeInterval ]}, { '$mod' : [{'$divide' : ['$ts', timeInterval ]},1] } ] } }, 
#                 "count_jobs": {"$sum":1},
#                 "count_ready": {"$sum":{ "$cond": [ {"$ne":["$status",1]},0,1]} },
#             #     count_running: {$sum:{ $cond: [ {$ne:["$status",2]},0,1]} },
#             #     count_error: {$sum:{ $cond: [ {$ne:["$status",4]},0,1]} },
#             #     count_paused: {$sum:{ $cond: [ {$ne:["$status",6]},0,1]} },
#                 }},
#             { "$project": {
#                 "avg_jobs": {"$divide": ["$count_jobs", (timeInterval/60)] },
#                 "avg_ready": {"$divide": ["$count_ready", (timeInterval/60)] },
#                 # avg_running: {$divide: ["$count_running",1] },
#                 # avg_error: {$divide: ["$count_error",1] },
#                 # avg_paused: {$divide: ["$count_paused",1] },
#                 # count_ready:1,
#                 # count_running:1,
#                 # count_error:1,
#                 # count_paused:1,
#                 # count_jobs:1,
#                 }},
#             { "$sort": {"_id":1}}
#             ])

    print "%s - %6.2f ms - request executed" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    prevTime = time.time()

    print "num doc retrieved: %d" % len(res["result"])
    return res



if __name__ == "__main__":

    ######
    startTime = time.time()
    prevTime = time.time()
    print ("%s - init timer" % (datetime.datetime.now()))

    options, args = createParser().parse_args()
   
    if options.verbose:
        print "Command options: %s" % options
        print "Command arguments: %s" % args

    # if len(args) is not 2:
    #     print "Error: 2 fields must be specified."
    #     sys.exit(1)
    # else:
    #     groupField = args[0]
    #     graphValue = args[1]

    startDate, endDate = getRangeDates( options )

    ######
    print "%s - %6.2f ms" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    prevTime = time.time()

    data = getData( startDate, endDate, options.resolution, options.filterExpr )

    job_queue = prepareGraph( options )

    arr_avg_jobs = [0] * len(data["result"])
    arr_avg_ready = [0] * len(data["result"])
    arr_avg_running = [0] * len(data["result"])
    arr_avg_error = [0] * len(data["result"])
    arr_avg_paused = [0] * len(data["result"])
    scale_avg_jobs = [''] * len(data["result"])

    for i, event in enumerate(data["result"]):
        # print "i:%d" %i
        arr_avg_jobs[i]=event["avg_jobs"]
        arr_avg_ready[i]=event["avg_ready"]
        arr_avg_running[i]=event["avg_running"]
        arr_avg_error[i]=event["avg_error"]
        arr_avg_paused[i]=event["avg_paused"]
        # scale_avg_jobs[i] = str(event["_id"]["timeInterval"])
        scale_avg_jobs[i] = datetime.datetime.fromtimestamp((options.resolution*event["_id"]["timeInterval"])).strftime('%Y-%m-%d %H:%M:%S')

    job_queue.add("avg jobs", arr_avg_jobs )
    job_queue.add("avg ready", arr_avg_ready )
    job_queue.add("avg running", arr_avg_running )
    job_queue.add("avg error", arr_avg_error )
    job_queue.add("avg paused", arr_avg_paused )
    job_queue.x_labels = scale_avg_jobs

    print "%s - %6.2f ms - add data" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    print "avg jobs:%r" % (arr_avg_jobs)
    print "avg ready:%r" % (arr_avg_ready)
    # prevTime = time.time()

    renderGraph( job_queue, options )


    ######
    print "%s - %6.2f ms - render graph" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    print "%s - %6.2f ms - Total time" % (datetime.datetime.now(), (time.time() - startTime) * 1000)
