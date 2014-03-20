#!/usr/bin/python2.6
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

###########################################################################################################################


def roundTime(dt=None, roundTo=60):
   """Round a datetime object to any time laps in seconds
   dt : datetime.datetime object, default now.
   roundTo : Closest number of seconds to round to, default 1 minute.
   Author: Thierry Husson 2012 - Use it as you want but don't blame me.
   """
   if dt == None : dt = datetime.datetime.now()
   seconds = (dt - dt.min).seconds

   rounding = (seconds+roundTo/2) // roundTo * roundTo
   return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)


def parseStats():
    """
    """
    pass

def process_args():
    '''
    Manages arguments parsing definition and help information
    '''

    usage = "usage: %prog [general options] [restriction list] [output option]"
    desc="""Displays information.
"""

    parser = OptionParser(usage=usage, description=desc, version="%prog 0.1" )

    parser.add_option( "-f", action="store", dest="sourceFile", help="Source file" )
    parser.add_option( "-o", action="store", dest="outputDir", default="./", help="Target output directory" )
    parser.add_option( "-v", action="store_true", dest="verbose", help="Verbose output" )
    parser.add_option( "-s", action="store", dest="rangeIn", type="int", help="Start range is N hours in past", default=3 )
    parser.add_option( "-e", action="store", dest="rangeOut", type="int", help="End range is N hours in past (mus be lower than '-s option'", default=0 )
    parser.add_option( "-r", "--res", action="store", dest="resolution", type="int", help="Indicates a period of time to aggregate data (in seconds)", default=1800 )
    # parser.add_option( "--last", action="store", dest="lastHours", type="int", help="Indicates a period of time to consider until current time", default=3 )
    parser.add_option( "--log", action="store_true", dest="logarithmic", help="Display graph with a logarithmic scale", default=False )
    parser.add_option( "--scale", action="store", dest="scaleEvery", type="int", help="Indicates the number of scale values to display", default=8 )

    # parser.add_option("-j", "--json", action="store_true", dest="json", help="Returns data formatted as JSON [%default]", default=False)
    options, args = parser.parse_args()

    return options, args



if __name__ == '__main__':

    # sourceFile = settings.LOGDIR+"/stats.log"

    options, args = process_args()
    VERBOSE = options.verbose
    
    if VERBOSE:
        print "Command options: %s" % options
        print "Command arguments: %s" % args

    sourceFile = options.sourceFile if options.sourceFile is not None else settings.LOGDIR+"/stats.log"

    # TODO allow start/end date or last N hours
    # startDate  = options.startDate if options.startDate is not None else (time.time() - 3600*2)
    # endDate    = options.endDate if options.endDate is not None else time.time()
    # startDate = time.time() - 3600 * options.lastHours

    if options.rangeIn < options.rangeOut:
        print "Invalid start/end range"
        sys.exit()

    startDate = time.time() - 3600 * options.rangeIn
    endDate = time.time() - 3600 * options.rangeOut
    # endDate = time.time()

    if VERBOSE:
        print "Loading stats: %r " % sourceFile
        print "  - from: %r " % datetime.date.fromtimestamp(startDate)
        print "  - to:   %r " % datetime.date.fromtimestamp(endDate)

    eventDate = 0
    scale=[]
    loopDuration=[]
    listTree=[]
    listRn=[]
    listDep=[]
    listDb=[]
    listCompute=[]
    listSend=[]
    listRelease=[]

    listIncomingRequest=[]
    listIncomingGet=[]
    listIncomingPost=[]
    listIncomingPut=[]
    listIncomingDelete=[]
    listAddGraphs=[]
    listAddRns=[]
    listUpdateCommands=[]

    avg_time_elapsed = 0
    avg_update_tree = 0
    avg_update_rn = 0
    avg_update_dependencies = 0
    avg_update_db = 0
    avg_compute_assignment = 0
    avg_send_assignment = 0
    avg_release_finishing = 0
    count_IncomingRequest = 0
    count_IncomingGet = 0
    count_IncomingPost = 0
    count_IncomingPut = 0
    count_IncomingDelete = 0
    count_AddGraphs = 0
    count_AddRns = 0
    count_UpdateCommands = 0

    with open( sourceFile , 'r') as f:
        statsReader = csv.reader(f,delimiter=';')
        
        for i,row in enumerate(statsReader):
            timestamp = float(row[0])

            if startDate < timestamp and timestamp <= endDate :

                date = datetime.datetime.fromtimestamp( timestamp )
                previousEventDate = eventDate
                eventDate = roundTime(date, options.resolution)

                # print " %r - %r = %r" % (eventDate, previousEventDate, (eventDate-previousEventDate)

                # Get average values of timers for the selected timer resolution
                avg_update_tree = ( avg_update_tree + float(row[1])*1000 ) / 2
                avg_update_rn = ( avg_update_rn + float(row[2])*1000 ) / 2
                avg_update_dependencies = ( avg_update_dependencies + float(row[3])*1000 ) / 2
                avg_update_db = ( avg_update_db + float(row[4])*1000 ) / 2
                avg_compute_assignment = ( avg_compute_assignment + float(row[5])*1000 ) / 2
                avg_send_assignment = ( avg_send_assignment + float(row[6])*1000 ) / 2
                avg_release_finishing = ( avg_release_finishing + float(row[7])*1000 ) / 2
                avg_time_elapsed = ( avg_time_elapsed + float(row[8])*1000 ) / 2

                # Sum number of event for the selected time resolution
                count_IncomingRequest += int(row[9])
                count_IncomingGet += int(row[10])
                count_IncomingPost += int(row[11])
                count_IncomingPut += int(row[12])
                count_IncomingDelete += int(row[13])
                count_AddGraphs += int(row[14])
                count_AddRns += int(row[15])
                count_UpdateCommands += int(row[16])

                #
                # Aggregate each "resolution" seconds i.e. when eventDate rounded changes
                #
                if eventDate != previousEventDate and previousEventDate is not 0:

                    scale.append( eventDate )

                    loopDuration.append(avg_time_elapsed)

                    listTree.append( avg_update_tree)
                    listRn.append(avg_update_rn)
                    listDep.append(avg_update_dependencies)
                    listDb.append(avg_update_db)
                    listCompute.append(avg_compute_assignment)
                    listSend.append(avg_send_assignment)
                    listRelease.append(avg_release_finishing)

                    listIncomingRequest.append( count_IncomingRequest / float(options.resolution) )
                    listIncomingGet.append( count_IncomingGet / float(options.resolution) )
                    listIncomingPost.append( count_IncomingPost / float(options.resolution) )
                    listIncomingPut.append( count_IncomingPut / float(options.resolution) )
                    listIncomingDelete.append( count_IncomingDelete / float(options.resolution) )
                    listAddGraphs.append( count_AddGraphs / float(options.resolution) )
                    listAddRns.append( count_AddRns / float(options.resolution) )
                    listUpdateCommands.append( count_UpdateCommands / float(options.resolution) )

                    #
                    # Reset values for next aggregation
                    #
                    avg_time_elapsed = 0
                    avg_update_tree = 0
                    avg_update_rn = 0
                    avg_update_dependencies = 0
                    avg_update_db = 0
                    avg_compute_assignment = 0
                    avg_send_assignment = 0
                    avg_release_finishing = 0
                    count_IncomingRequest = 0
                    count_IncomingGet = 0
                    count_IncomingPost = 0
                    count_IncomingPut = 0
                    count_IncomingDelete = 0
                    count_AddGraphs = 0
                    count_AddRns = 0
                    count_UpdateCommands = 0


    line_chart = pygal.Line( x_label_rotation=40, logarithmic=options.logarithmic )
    line_chart.title = 'Average loop duration (in ms)'


    # Adapt date values to have a useful time scale
    (eventDate, previousDate) = (0,0)
    strScale=[]
    for i, val in enumerate(scale):
        previousDate = eventDate
        eventDate = val

        if previousDate == 0 or (eventDate.day - previousDate.day) > 0 or i == len(scale)-1:
            scaleValue = eventDate.strftime('%Y-%m-%d %H:%M')
        else:
            if len(scale) < options.scaleEvery:
                scaleValue = eventDate.strftime('%H:%M')
            else:
                if ( i % (len(scale)/options.scaleEvery) ) == 0:
                    scaleValue = eventDate.strftime('%H:%M')
                else:
                    scaleValue = ""

        strScale.append( scaleValue )


    line_chart.x_labels = strScale
    # line_chart.x_labels = scale

    line_chart.add('Total in loop',  loopDuration)
    line_chart.add('Update tree',  listTree)
    # line_chart.add('Update rn',  listRn)
    line_chart.add('Update db',  listCompute)
    line_chart.add('Dependencies',  listDep)
    line_chart.add('Assignment',  listSend)
    # line_chart.add('Send order',  listSend)
    # line_chart.add('Release finished',  listRelease)

    line_chart.render_to_file( os.path.join(options.outputDir, "timers.svg") )


    line_chart = pygal.Line( x_label_rotation=45, logarithmic=options.logarithmic )
    line_chart.title = 'Requests by second (avg over %d s)' % options.resolution
    line_chart.x_labels = strScale
    line_chart.add('All requests', listIncomingRequest )
    line_chart.add('- GET', listIncomingGet )
    line_chart.add('- POST', listIncomingPost )
    line_chart.add('- PUT', listIncomingPut )
    # line_chart.add('DELETE', listIncomingDelete )
    line_chart.add('Add graphs', listAddGraphs )
    # line_chart.add('New RNs', listAddRns )
    line_chart.add('Update commands', listUpdateCommands )
 
    line_chart.render_to_file( os.path.join(options.outputDir, "requests.svg") )

    