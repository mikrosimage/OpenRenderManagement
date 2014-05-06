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

import numpy as np
import pygal
from pygal.style import *

try:
    import simplejson as json
except ImportError:
    import json


from octopus.dispatcher import settings
from octopus.core import singletonconfig
from tools.common import roundTime
from tools.common import lowerQuartile, higherQuartile
from tools.stats.common import createCommonParser, getRangeDates, prepareGraph, prepareScale, renderGraph


###########################################################################################################################
# Data example:
# {
#     "prod":{
#           "ddd" :     { "jobs":15, "err":1, "paused":2, "ready/blocked":10, "running":2, "allocatedRN":5, "readyCommandCount":15},
#           "dior_tea" :    { "jobs":1, "err":0, "paused":0, "ready/blocked":0, "running":1, "allocatedRN":1, "readyCommandCount":15},
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



if __name__ == "__main__":

    # # DBG
    # startTime = time.time()
    # prevTime = time.time()
    # print ("%s - init timer" % (datetime.datetime.now()))

    options, args = createCommonParser().parse_args()
   
    if options.verbose:
        print "Command options: %s" % options
        print "Command arguments: %s" % args

    if len(args) is not 2:
        print "Error: 2 fields must be specified."
        sys.exit(1)
    else:
        groupField = args[0]
        graphValue = args[1]


    startDate, endDate = getRangeDates( options )
    
    if options.verbose:
        print "Loading stats: %r " % options.sourceFile
        print "  - from: %r " % datetime.date.fromtimestamp(startDate)
        print "  - to:   %r " % datetime.date.fromtimestamp(endDate)
        print "Start."

    strScale=[]
    scale=[]
    data2Dim = {}
    log = []

    #
    # Load json log and filter by date
    # Optim done to have a fast dataset:
    #  - read the whole file without parsing
    #  - read resulting list in reversed order and parse each json line (mandatory due to the log format)
    #  - filter and add data in range
    #  - once we reached data too old: break the loop

    with open(options.sourceFile, "r" ) as f:
        raw_str = f.readlines()

    # print "%s - %6.2f ms - load raw source complete, num lines: %d" % (datetime.datetime.now(), (time.time() - prevTime) * 1000,len(raw_str))
    # prevTime = time.time()

    # for line in reversed(raw_str):
    #     date = float(re.search('"requestDate":(.+?),', line).group(1))
    #     if (startDate < date  and date <= endDate):
    #         log.insert( 0, json.loads(line) )
    #     if date < startDate:
    #         break

    for line in reversed(raw_str):
        data = json.loads(line)

        if (startDate < data['requestDate']  and data['requestDate'] <= endDate):
            log.insert( 0, data )

        # We read by the end, if date is too old, no need to continue the parsing
        if data['requestDate'] < startDate:
            break

    # print "%s - %6.2f ms - load source complete, num lines: %d" % (datetime.datetime.now(), (time.time() - prevTime) * 1000, len(log))
    # prevTime = time.time()


    for i, data in enumerate(log):
        eventDate = datetime.datetime.fromtimestamp( data['requestDate'] )

        for key, val in data[ groupField ].items():
            if key not in data2Dim:
                data2Dim[key] = np.array( [0]*len(log) )
            data2Dim[key][i] = val[ graphValue ]

        scale.append( eventDate )
    # print "%s - %6.2f ms - create tables" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    # prevTime = time.time()

    stepSize = len(scale) / options.resolution
    newshape = (options.resolution, stepSize)
    useableSize = len(scale) - ( len(scale) % options.resolution )

    avgData = {}

    if options.verbose:
        print "stepSize=%d" % stepSize
        print "useableSize=%d" % useableSize

    for dataset in data2Dim.keys():
        # print "%s = %d - %r" % (dataset, len(data2Dim[dataset]), data2Dim[dataset])
        avgData[dataset] = np.mean( np.reshape(data2Dim[dataset][-useableSize:], newshape), axis=1)

    # print "%s - %6.2f ms - create avg data" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    # prevTime = time.time()

    # working = np.array(nb_working[-useableSize:])
    # unknown = np.array(nb_unknown[-useableSize:])
    # paused = np.array(nb_paused[-useableSize:])


    # # print ("working %d = %r" % (len(working), working) )
    # # print ("reshape %d = %r" % (len(newshape), newshape) )

    # avg_working= np.mean( np.reshape(working, newshape), axis=1)
    # avg_paused= np.mean( np.reshape(paused, newshape), axis=1)
    # avg_unknown= np.mean( np.reshape(unknown, newshape), axis=1)


    # # med= np.median(data, axis=1)
    # # amin= np.min(data, axis=1)
    # # amax= np.max(data, axis=1)
    # # q1= lowerQuartile(data)
    # # q2= higherQuartile(data)
    # # std= np.std(data, axis=1)

    # strScale = [''] * options.resolution
    tmpscale = np.reshape(scale[-useableSize:], newshape)
    strScale = prepareScale( tmpscale, options )

    # for i,date in enumerate(tmpscale[::len(tmpscale)/options.scaleEvery]):
    #     newIndex = i*len(tmpscale)/options.scaleEvery

    #     if newIndex < len(strScale):
    #         strScale[newIndex] = date[0].strftime('%H:%M')

    # strScale[0] = scale[0].strftime('%Y-%m-%d %H:%M')
    # strScale[-1] = scale[-1].strftime('%Y-%m-%d %H:%M')

    # print "%s - %6.2f ms - create scale" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    # prevTime = time.time()

    if options.verbose:
        print ("newshape %d = %r" % (len(newshape), newshape) )
        print ("data2Dim %d = %r" % (len(data2Dim), data2Dim) )
        print ("scale %d = %r" % (len(strScale), strScale) )

    if options.verbose:
        print "Num events: %d" % len(scale)
        print "Creating graph."


    avg_usage = prepareGraph( options )
    avg_usage.title = options.title
    avg_usage.x_labels = strScale

    for key,val in avgData.items():
        avg_usage.add(key, val )
    # print "%s - %6.2f ms - prepare graph" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    # prevTime = time.time()

    renderGraph( avg_usage, options )
    # print "%s - %6.2f ms - render graph" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    # print "%s - %6.2f ms - Total time" % (datetime.datetime.now(), (time.time() - startTime) * 1000)

    if options.verbose:
        print "Done."

