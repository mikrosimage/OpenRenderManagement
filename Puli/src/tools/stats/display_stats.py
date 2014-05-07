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
from tools.common import roundTime

###########################################################################################################################

# Data example:
# 1393227003.538171;0.000845;0.000659;0.000012;0.000029;0.047723;0.011724;0.000304;0.063231;136;0;0;136;0;0;0;0;105
# 1393227007.538166;0.024805;0.001920;0.017431;0.330579;0.023362;0.009779;0.000298;0.411167;302;0;0;302;0;0;0;170;46
# 1393227011.538171;0.024706;0.001671;0.000013;0.153579;0.002286;0.000011;0.000261;0.184581;231;1;0;230;0;0;0;97;0
# 1393227015.538167;0.022484;0.001105;0.000013;0.004701;0.001727;0.000010;0.000218;0.032021;147;1;0;146;0;0;0;12;0
# 1393227019.538172;0.023493;0.001282;0.000014;0.002844;0.002301;0.000011;0.000303;0.032382;140;2;0;138;0;0;0;5;0
# 1393227023.538168;0.023865;0.001234;0.000013;0.000882;0.001866;0.000010;0.000229;0.030033;134;0;0;134;0;0;0;1;0
# 1393227027.538255;0.022949;0.001238;0.000014;0.001049;0.002340;0.000012;0.000269;0.030133;134;0;0;134;0;0;0;1;0
# 1393227031.538172;0.022315;0.001112;0.000011;0.000827;0.001713;0.000010;0.000219;0.027970;133;0;0;133;0;0;0;1;0
# 1393227035.538171;0.000833;0.001247;0.000012;0.000026;0.002080;0.000010;0.000228;0.006208;135;2;0;133;0;0;0;0;0
# 1393227039.538170;0.022118;0.001121;0.017598;0.059938;0.003182;0.000061;0.000250;0.127258;139;5;1;133;0;1;0;0;1
# 1393227043.538191;0.025142;0.001268;0.033692;0.073282;0.002914;0.000054;0.000209;0.139076;146;4;1;141;0;1;0;9;1


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
    parser.add_option( "--suffix", action="store", dest="suffix", type="string", help="Suffix added to output file name", default='')
    parser.add_option( "-e", action="store", dest="rangeOut", type="int", help="End range is N hours in past (mus be lower than '-s option'", default=0 )
    parser.add_option( "-r", "--res", action="store", dest="resolution", type="int", help="Indicates a period of time to aggregate data (in seconds)", default=1800 )
    parser.add_option( "--log", action="store_true", dest="logarithmic", help="Display graph with a logarithmic scale", default=False )
    parser.add_option( "--scale", action="store", dest="scaleEvery", type="int", help="Indicates the number of scale values to display", default=8 )

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


    line_chart = pygal.StackedLine( x_label_rotation=40, logarithmic=options.logarithmic, show_dots=False, fill=True, width=800, height=300 )
    line_chart.title = 'Average loop duration (in ms)'


    # Adapt date values to have a useful time scale


    #     strScale.append( scaleValue )

    strScale = []
    options.scaleEvery = min(options.scaleEvery, options.resolution )
    for i, date in enumerate(scale):
        # print "i:%d - %s" % (i,date)
        if i < len(scale):
            strScale.append( date.strftime('%H:%M') )

    padding = len(scale)/options.scaleEvery
    for i, date in enumerate(strScale):
        if i%padding != 0:
            strScale[i] = ''

    strScale[0] = scale[0].strftime('%m-%d %H:%M')
    strScale[-1] = scale[-1].strftime('%m-%d %H:%M')


    line_chart.x_labels = strScale

    # line_chart.add('Total in loop',  loopDuration)
    line_chart.add('Compute Assignment',  listCompute)
    line_chart.add('Update status & progress',  listTree)
    line_chart.add('Update db',  listDb)
    line_chart.add('Dependencies',  listDep)
    line_chart.add('Send order to RN',  listSend)

    line_chart.render_to_file( os.path.join(options.outputDir, "timers"+options.suffix+".svg") )


    line_chart = pygal.Line( x_label_rotation=45, logarithmic=options.logarithmic, width=800, height=300 )
    line_chart.title = 'Requests by second (avg over %d s)' % options.resolution
    line_chart.x_labels = strScale
    # line_chart.add('All requests', listIncomingRequest )
    line_chart.add('- GET', listIncomingGet )
    line_chart.add('- POST', listIncomingPost )
    line_chart.add('- PUT', listIncomingPut )
    # line_chart.add('DELETE', listIncomingDelete )
    line_chart.add('Add graphs', listAddGraphs )
    # line_chart.add('New RNs', listAddRns )
    line_chart.add('Update commands', listUpdateCommands )
 
    line_chart.render_to_file( os.path.join(options.outputDir, "requests"+options.suffix+".svg") )

    