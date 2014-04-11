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


def process_args():
    '''
    Manages arguments parsing definition and help information
    '''

    usage = "usage: %prog [general options] [restriction list] [output option]"
    desc="""Displays information.
"""

    parser = OptionParser(usage=usage, description=desc, version="%prog 0.1" )

    parser.add_option( "-f", action="store", dest="sourceFile", default=os.path.join(settings.LOGDIR, "usage_stats.log"), help="Source file" )
    parser.add_option( "-o", action="store", dest="outputFile", default="./usage_avg.svg", help="Target output file." )
    parser.add_option( "--render-mode", action="store", dest="renderMode", type="string", help="render destination: inline, svg or png", default="svg" )

    parser.add_option( "-v", action="store_true", dest="verbose", help="Verbose output" )
    parser.add_option( "-s", action="store", dest="rangeIn", type="int", help="Start range is N hours in past", default=3 )
    parser.add_option( "-e", action="store", dest="rangeOut", type="int", help="End range is N hours in past (mus be lower than '-s option'", default=0 )

    parser.add_option( "--startTime", action="store", dest="timeIn", type="int", help="Start range is at timestamp", default=0 )
    parser.add_option( "--endTime",   action="store", dest="timeOut", type="int", help="End range is at timestamp", default=time.time() )

    parser.add_option( "-t", "--title", action="store", dest="title", help="Indicates a title", default="RN usage over time")
    parser.add_option( "-r", "--res", action="store", dest="resolution", type="int", help="Indicates ", default=10 )
    parser.add_option( "--stack", action="store_true", dest="stacked", default=False)
    parser.add_option( "--line", action="store_true", dest="line", default=True)
    parser.add_option( "--log", action="store_true", dest="logarithmic", help="Display graph with a logarithmic scale", default=False )
    parser.add_option( "--style", action="store", dest="style", help="Set a specific style name (BlueStyle, RedBlueStyle ...)", default="RedBlue" )
    parser.add_option( "--scale", action="store", dest="scaleEvery", type="int", help="Indicates the number of scale values to display", default=8 )

    parser.add_option( "--hide-offline", action="store_false", dest="offline", help="", default=True )
    parser.add_option( "--hide-paused", action="store_false", dest="paused", help="", default=True )
    parser.add_option( "--hide-working", action="store_false", dest="working", help="", default=True )
    parser.add_option( "--hide-idle", action="store_false", dest="idle", help="", default=True )

    options, args = parser.parse_args()

    return options, args


if __name__ == "__main__":


    options, args = process_args()
    VERBOSE = options.verbose
    
    if VERBOSE:
        print "Command options: %s" % options
        print "Command arguments: %s" % args

    if options.timeIn is not 0:
        if VERBOSE:
            print "Using precise time range: [%s - %s]" % (options.timeIn,options.timeOut)
        startDate = options.timeIn
        endDate = options.timeOut

        if options.timeOut < options.timeIn:
            print "Invalid start/end range"
            sys.exit()
    else:
        if options.rangeIn < options.rangeOut:
            print "Invalid start/end range"
            sys.exit()
        startDate = time.time() - 3600 * options.rangeIn
        endDate = time.time() - 3600 * options.rangeOut

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
            log.append( json.loads(line) )

    
    for data in log:
        eventDate = datetime.datetime.fromtimestamp( data['date'] )

        if data['date'] < startDate or endDate <= data['date'] :
            continue

        nb_working.append(data["rendernodes"]["renderNodesByStatus"]['Working'] + data["rendernodes"]["renderNodesByStatus"]['Assigned'])
        nb_paused.append(data["rendernodes"]["renderNodesByStatus"]['Paused'])
        nb_unknown.append(data["rendernodes"]["renderNodesByStatus"]['Unknown'])
        nb_idle.append(data["rendernodes"]["renderNodesByStatus"]['Idle'])

        scale.append( eventDate )

    if VERBOSE:
        print "Num events: %d" % len(scale)

    if len(scale) < options.resolution:
        if VERBOSE:
            print "Too few events for resolution or scale: limit to %d" % len(scale)
        options.resolution = len(scale)


    stepSize = len(scale) / options.resolution
    newshape = (options.resolution, stepSize)
    useableSize = len(scale) - ( len(scale) % options.resolution )

    
    working = np.array(nb_working[-useableSize:])
    unknown = np.array(nb_unknown[-useableSize:])
    paused = np.array(nb_paused[-useableSize:])
    idle = np.array(nb_idle[-useableSize:])


    avg_working= np.around( np.mean( np.reshape(working, newshape), axis=1), decimals=0)
    avg_paused= np.around( np.mean( np.reshape(paused, newshape), axis=1), decimals=0)
    avg_unknown= np.around( np.mean( np.reshape(unknown, newshape), axis=1), decimals=0)
    avg_idle= np.around( np.mean( np.reshape(idle, newshape), axis=1), decimals=0)


    # med= np.median(data, axis=1)
    # amin= np.min(data, axis=1)
    # amax= np.max(data, axis=1)
    # q1= lowerQuartile(data)
    # q2= higherQuartile(data)
    # std= np.std(data, axis=1)


    strScale = [''] * options.resolution
    tmpscale = np.reshape(scale[-useableSize:], newshape)
    # print ("tmp scale %d = %r" % (len(tmpscale), tmpscale) )
    # print ("str scale %d = %r" % (len(strScale), strScale) )

    options.scaleEvery = min(options.scaleEvery, options.resolution )
    for i,date in enumerate(tmpscale[::len(tmpscale)/options.scaleEvery]):
        newIndex = i*len(tmpscale)/options.scaleEvery

        if newIndex < len(strScale):
            strScale[newIndex] = date[0].strftime('%H:%M')

    strScale[0] = scale[0].strftime('%Y-%m-%d %H:%M')
    strScale[-1] = scale[-1].strftime('%Y-%m-%d %H:%M')

    if VERBOSE:
        print ("newshape %d = %r" % (len(newshape), newshape) )
        print ("avg %d = %r" % (len(avg_working), avg_working) )
        print ("scale %d = %r" % (len(strScale), strScale) )

    # sert a etablir une distribution des valeurs du tableau dans chaque "bin"
    # hist, bin_edges = np.histogram(a, [0,10,20,30,40,50])

    style = { "RedBlue": RedBlueStyle,
              "Blue" : BlueStyle,
              "Light" : LightStyle,
              "Default": DefaultStyle,
              "Clean": CleanStyle,
              "DarkColorized": DarkColorizedStyle,
              "DarkGreenBlue": DarkGreenBlueStyle,
              # "DarkGreen": DarkGreenStyle,
              # "LightSolarized": LightSolarizedStyle,
              # "Neon": NeonStyle,
            }

    if options.style not in style.keys():
        print "Error: Style is not recognised, using \"default\" instead"
        options.style = "Default"

    if options.stacked:
        avg_usage = pygal.StackedLine( x_label_rotation=30,
                                include_x_axis=True,
                                logarithmic=options.logarithmic, 
                                show_dots=False,
                                width=800, 
                                height=300,
                                fill=True,
                                interpolate='hermite', 
                                interpolation_parameters={'type': 'cardinal', 'c': 1.0},
                                interpolation_precision=3,
                                style=style[options.style]
                                )
    else:
        avg_usage = pygal.Line( x_label_rotation=30,
                                include_x_axis=True,
                                logarithmic=options.logarithmic, 
                                show_dots=True,
                                width=800, 
                                height=300,
                                interpolate='hermite', 
                                interpolation_parameters={'type': 'cardinal', 'c': 1.0},
                                interpolation_precision=3,
                                style=style[options.style]
                                )

    avg_usage.title = options.title
    avg_usage.x_labels = strScale

    if options.offline:
        avg_usage.add('Offline', avg_unknown )
    if options.paused:
        avg_usage.add('Paused', avg_paused )
    if options.working:
        avg_usage.add('Working', avg_working )
    if options.idle:
        avg_usage.add('Idle', avg_idle )

    if options.renderMode == 'svg':
        avg_usage.render_to_file( options.outputFile )
    elif options.renderMode == 'png':
        avg_usage.render_to_png( options.outputFile )
    elif options.renderMode == 'inline':
        print avg_usage.render()
