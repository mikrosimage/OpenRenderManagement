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
from tools.common import lowerQuartile, higherQuartile
from tools.stats.common import createCommonParser, getRangeDates, prepareGraph, prepareScale, renderGraph

# import matplotlib.pyplot as plt

###########################################################################################################################
# {
#     "date": timestamp
#     "licenses": "{\"shave\" : \"0 / 70\",\"nuke\" : \"0 / 70\",\"clarisse\" : \"0 / 5\",\"mtoa\" : \"137 / 195\",\"katana\" : \"24 / 200\",\"ocula\" : \"0 / 3\"}",
#     "rendernodes": 
#         {
#             "renderNodesByStatus": 
#                 {
#                     "Paused": 89, 
#                     "Working": 152, 
#                     "Unknown": 51, 
#                     "Assigned": 0, 
#                     "Idle": 15, 
#                     "Booting": 0, 
#                     "Finishing": 0
#                 },
#             "totalCores": 5192, 
#             "missingRenderNodes": 51, 
#             "idleCores": 1844
#         }, 
#         "commands": 
#                     {
#                         "ASSIGNED": 0, 
#                         "CANCELED": 38926, 
#                         "RUNNING": 151, 
#                         "DONE": 67467, 
#                         "TIMEOUT": 0, 
#                         "ERROR": 115, 
#                         "READY": 5455, 
#                         "FINISHING": 0, 
#                         "TOTAL": 117238, 
#                         "BLOCKED": 5124
#                     },
#         "jobs": 
#             {
#                 "total": 2519
#             }
# }



def parseFarmArgs( commonParser ):
    '''
    Manages arguments parsing definition and help information
    '''

    commonParser.add_option( "--hide-offline", action="store_false", dest="offline", help="", default=True )
    commonParser.add_option( "--hide-paused", action="store_false", dest="paused", help="", default=True )
    commonParser.add_option( "--hide-working", action="store_false", dest="working", help="", default=True )
    commonParser.add_option( "--hide-idle", action="store_false", dest="idle", help="", default=True )

    options, args = commonParser.parse_args()
    return options, args


if __name__ == "__main__":

    # DBG
    # startTime = time.time()
    # prevTime = time.time()

    # print ("%s - init timer" % (datetime.datetime.now()))

    options, args = parseFarmArgs( createCommonParser() )
    VERBOSE = options.verbose
    
    if VERBOSE:
        print "Command options: %s" % options
        print "Command arguments: %s" % args

    startDate, endDate = getRangeDates( options )

    if VERBOSE:
        print "Loading stats: %r " % options.sourceFile
        print "  - from: %r " % datetime.date.fromtimestamp(startDate)
        print "  - to:   %r " % datetime.date.fromtimestamp(endDate)
        print "Start."

    nbjobs=[]
    nb_working=[]
    nb_paused=[]
    nb_idle=[]
    nb_assigned=[]
    nb_unknown=[]
    nb_booting=[]
    nb_finishing=[]
    strScale=[]
    scale=[]

    log = []
    with open(options.sourceFile, "r" ) as f:
        for line in f:
            item = json.loads(line)
            if item['date'] < startDate or endDate <= item['date'] :
                continue
            log.append( item )

    # print "%s - %6.2f ms - load source complete" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    # prevTime = time.time()
    
    for data in log:
        eventDate = datetime.datetime.fromtimestamp( data['date'] )


        if options.working:
            nb_working.append(data["rendernodes"]["renderNodesByStatus"]['Working'] + data["rendernodes"]["renderNodesByStatus"]['Assigned'])

        if options.paused:
            nb_paused.append(data["rendernodes"]["renderNodesByStatus"]['Paused'])

        if options.offline:
            nb_unknown.append(data["rendernodes"]["renderNodesByStatus"]['Unknown'])

        if options.idle:
            nb_idle.append(data["rendernodes"]["renderNodesByStatus"]['Idle'])

        scale.append( eventDate )

    # print "%s - %6.2f ms - create temp array" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    # prevTime = time.time()


    if VERBOSE:
        print "Num events: %d" % len(scale)

    if len(scale) < options.resolution:
        if VERBOSE:
            print "Too few events for resolution or scale: limit to %d" % len(scale)
        options.resolution = len(scale)


    stepSize = len(scale) / options.resolution
    newshape = (options.resolution, stepSize)
    useableSize = len(scale) - ( len(scale) % options.resolution )

    # print "%s - %6.2f ms - create newshape" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    # prevTime = time.time()

    

    if options.working:
        working = np.array(nb_working[-useableSize:])
        avg_working= np.around( np.mean( np.reshape(working, newshape), axis=1), decimals=0)

    if options.offline:
        unknown = np.array(nb_unknown[-useableSize:])
        avg_unknown= np.around( np.mean( np.reshape(unknown, newshape), axis=1), decimals=0)

    if options.paused:
        paused = np.array(nb_paused[-useableSize:])
        avg_paused= np.around( np.mean( np.reshape(paused, newshape), axis=1), decimals=0)

    if options.idle:
        idle = np.array(nb_idle[-useableSize:])
        avg_idle= np.around( np.mean( np.reshape(idle, newshape), axis=1), decimals=0)

    # print "%s - %6.2f ms - create and average numpy arrays" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    # prevTime = time.time()

    # med= np.median(data, axis=1)
    # amin= np.min(data, axis=1)
    # amax= np.max(data, axis=1)
    # q1= lowerQuartile(data)
    # q2= higherQuartile(data)
    # std= np.std(data, axis=1)


    #
    # Prepare scale
    #
    tmpscale = np.reshape(scale[-useableSize:], newshape)
    strScale = prepareScale( tmpscale, options )
    # print ("scale %d = %r" % (len(strScale), strScale) )


    if VERBOSE:
        print ("newshape %d = %r" % (len(newshape), newshape) )
        print ("avg %d = %r" % (len(avg_working), avg_working) )
        print ("scale %d = %r" % (len(strScale), strScale) )

    graph = prepareGraph( options )
    graph.title = options.title
    graph.x_labels = strScale

    if options.offline:
        graph.add('Offline', avg_unknown )
    if options.paused:
        graph.add('Paused', avg_paused )
    if options.working:
        graph.add('Working', avg_working )
    if options.idle:
        graph.add('Idle', avg_idle )

    # print "%s - %6.2f ms - prepare graph" % (datetime.datetime.now(), (time.time()-prevTime) * 1000)
    # prevTime = time.time()

    renderGraph( graph, options )

    # print "%s - %6.2f ms - render graph" % (datetime.datetime.now(), (time.time() - prevTime) * 1000)
    # print "%s - %6.2f ms - Total time" % (datetime.datetime.now(), (time.time() - startTime) * 1000)

    if options.verbose:
        print "Done."