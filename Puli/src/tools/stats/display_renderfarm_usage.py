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
import pygal
from optparse import OptionParser

from octopus.dispatcher import settings
from octopus.core import singletonconfig

try:
    import simplejson as json
except ImportError:
    import json

from tools.common import roundTime
import numpy as np
import matplotlib.pyplot as plt

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
    parser.add_option( "-o", action="store", dest="outputDir", default="./", help="Target output directory" )
    parser.add_option( "-v", action="store_true", dest="verbose", help="Verbose output" )
    parser.add_option( "-s", action="store", dest="rangeIn", type="int", help="Start range is N hours in past", default=3 )
    parser.add_option( "-e", action="store", dest="rangeOut", type="int", help="End range is N hours in past (mus be lower than '-s option'", default=0 )
    parser.add_option( "-r", "--res", action="store", dest="resolution", type="int", help="Indicates a period of time to aggregate data (in seconds)", default=1800 )
    parser.add_option( "--log", action="store_true", dest="logarithmic", help="Display graph with a logarithmic scale", default=False )
    parser.add_option( "--scale", action="store", dest="scaleEvery", type="int", help="Indicates the number of scale values to display", default=8 )

    options, args = parser.parse_args()

    return options, args


if __name__ == "__main__":


    options, args = process_args()
    VERBOSE = options.verbose
    
    if VERBOSE:
        print "Command options: %s" % options
        print "Command arguments: %s" % args

    
    if options.rangeIn < options.rangeOut:
        print "Invalid start/end range"
        sys.exit()

    startDate = time.time() - 3600 * options.rangeIn
    endDate = time.time() - 3600 * options.rangeOut

    if VERBOSE:
        print "Loading stats: %r " % options.sourceFile
        print "  - from: %r " % datetime.date.fromtimestamp(startDate)
        print "  - to:   %r " % datetime.date.fromtimestamp(endDate)

    print "start..."

    nbjobs=[]
    nb_working=[]
    nb_paused=[]
    nb_idle=[]
    nb_assigned=[]
    nb_unknown=[]
    nb_booting=[]
    nb_finishing=[]
    strScale=[]

    log = []
    with open(options.sourceFile, "r" ) as f:
        for line in f:
            log.append( json.loads(line) )

    
    for data in log:
        eventDate = datetime.datetime.fromtimestamp( data['date'] )
        # eventDate = roundTime(data['date'], options.resolution)
        print "%s - %r" % (eventDate, data["rendernodes"]["renderNodesByStatus"] )
        nb_working.append(data["rendernodes"]["renderNodesByStatus"]['Working'])
        nb_paused.append(data["rendernodes"]["renderNodesByStatus"]['Paused'])
        nb_unknown.append(data["rendernodes"]["renderNodesByStatus"]['Unknown'])
        nb_assigned.append(data["rendernodes"]["renderNodesByStatus"]['Assigned'])
        nb_idle.append(data["rendernodes"]["renderNodesByStatus"]['Idle'])
        nb_booting.append(data["rendernodes"]["renderNodesByStatus"]['Booting'])
        nb_finishing.append(data["rendernodes"]["renderNodesByStatus"]['Finishing'])



        strScale.append( str(eventDate) )
    print "nb events: %d" % len(log)
    print "done..."

    ######
    a = np.array(nb_working[:10])
    plt.axes(strScale[:10])
    plt.plot(strScale[:10], nb_working[:10])
    plt.show()
    ######

    usage = pygal.Line( x_label_rotation=40, logarithmic=False )
    usage.title = 'RN usage over time'
    usage.x_labels = strScale

    # usage.add('nb jobs', nbjobs )
    usage.add('Working', nb_working )
    usage.add('Paused', nb_paused )
    # usage.add('Unknown', nb_unknown )
    usage.add('Assigned', nb_assigned )
    usage.add('Iddle', nb_idle )
    # usage.add('Booting', nb_booting )
    # usage.add('Finishing', nb_finishing )

    usage.render_to_file( os.path.join("/s/prods/ddd/_sandbox/jsa/stats", "usage.svg") )
