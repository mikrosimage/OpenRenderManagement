#!/usr/bin/python2.6
# -*- coding: utf8 -*-

"""

"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2014, Mikros Image"

import logging
import logging.handlers

import os
from copy import copy
try:
    import simplejson as json
except ImportError:
    import json

from octopus.core import singletonconfig
from octopus.dispatcher import settings

# 
# Load specific logger for collecting stats. 
#

# Custom level to avoid flooding the main loggers
# We use a logger and handler with low level to ensure it always receive message even in log level is change 
# via the config file and reloaded.
hd = logging.handlers.RotatingFileHandler( os.path.join(settings.LOGDIR, "stats.log"), maxBytes=singletonconfig.get('CORE','STATS_SIZE'), backupCount=0 )
hd.setFormatter( logging.Formatter('%(message)s') )
hd.setLevel( 1 )

statsLog = logging.getLogger('server_stats')
statsLog.addHandler( hd )
statsLog.setLevel( 1 )


class DispatcherStats():
    """
    | Class holding custom infos on the dispatcher.
    | This data can be periodically flushed in a specific log file for later use
    """

    cycleDate = 0.0
    
    cycleTimers = {
        'update_tree':0.0,
        'update_rn':0.0,
        'update_dependencies':0.0,
        'update_db':0.0,
        'compute_assignment':0.0,
        'send_assignment':0.0,
        'release_finishing':0.0,
        'time_elapsed':0.0,
    }

    cycleCounts = {
        'incoming_requests': 0,
        'incoming_get': 0,
        'incoming_post': 0,
        'incoming_put': 0,
        'incoming_delete': 0,
        'add_graphs': 0,
        'add_rns': 0,
        'update_commands': 0,
        'num_assignments': 0,
    }

    assignmentTimers = {
        'update_max_rn':0.0,
        'dispatch_command':0.0,
    }

    accumulationBuffer = []


    def __init__( self, *args, **kwargs ):
        pass

    def _resetCounts( self ):
        for key in self.cycleCounts.keys():
            self.cycleCounts[key] = 0


    def aggregate( self ):
        """
        | Called each cycle to store data in a buffer array
        | Once every BUFFER_SIZE cycles, the data is dumped in stats.log for later use
        """

        cycleData = [ self.cycleDate, copy( self.cycleTimers ), copy(self.cycleCounts), copy( self.assignmentTimers ) ]
        self.accumulationBuffer.append( cycleData )

        # Clean data for next cycle (only counts need to be cleaned, timer are overwritten)
        self._resetCounts()

        # Dump to file
        if singletonconfig.get('CORE','STATS_BUFFER_SIZE') <= len(self.accumulationBuffer):
            self._flush()

        return True


    def _flush( self ):
        """
        TODO flush in a dedicated thread
        """
        for line in self.accumulationBuffer:
            statsLog.log( 1,
                "%f;%f;%f;%f;%f;%f;%f;%f;%f;%d;%d;%d;%d;%d;%d;%d;%d;%d;%d;%d" % ( 
                line[0], 

                line[1]['update_tree'],         # from dispatchLoop
                line[1]['update_rn'],           # from dispatchLoop
                line[1]['update_dependencies'], # from dispatchLoop
                line[1]['update_db'],           # from dispatchLoop
                line[1]['compute_assignment'],  # from dispatchLoop
                line[1]['send_assignment'],     # from dispatchLoop
                line[1]['release_finishing'],   # from dispatchLoop
                line[1]['time_elapsed'],        # from dispatchLoop

                line[2]['incoming_requests'],   # from base ressource handler
                line[2]['incoming_get'],        # from base ressource handler
                line[2]['incoming_post'],       # from base ressource handler
                line[2]['incoming_put'],        # from base ressource handler
                line[2]['incoming_delete'],     # from base ressource handler

                line[2]['add_graphs'],          # from WS add graph
                line[2]['add_rns'],             # from WS rendernodes
                line[2]['update_commands'],     # from WS rendernodes
                line[2]['num_assignments'],     # from dispatchLoop

                line[3]['update_max_rn'],       # from dispatchLoop in computeAssignment
                line[3]['dispatch_command'],    # from dispatchLoop in computeAssignment
                )
            )

        # with open( os.path.join(settings.LOGDIR, "stats.log") , 'a') as f:
        #     for line in self.accumulationBuffer:

        #         f.write(
        #             "%f;%f;%f;%f;%f;%f;%f;%f;%f;%d;%d;%d;%d;%d;%d;%d;%d;%d;%d;%d\n" % ( 
        #                 line[0],                            # 0

        #                 line[1]['update_tree'],             # 1
        #                 line[1]['update_rn'],               # 2
        #                 line[1]['update_dependencies'],     # 3
        #                 line[1]['update_db'],               # 4
        #                 line[1]['compute_assignment'],      # 5
        #                 line[1]['send_assignment'],         # 6
        #                 line[1]['release_finishing'],       # 7
        #                 line[1]['time_elapsed'],            # 8

        #                 line[2]['incoming_requests'],       # 9
        #                 line[2]['incoming_get'],            # 10
        #                 line[2]['incoming_post'],           # 11
        #                 line[2]['incoming_put'],            # 12
        #                 line[2]['incoming_delete'],         # 13

        #                 line[2]['add_graphs'],              # 14
        #                 line[2]['add_rns'],                 # 15
        #                 line[2]['update_commands'],         # 16
        #                 line[2]['num_assignments'],         # 17

        #                 line[3]['update_max_rn'],           # 18
        #                 line[3]['dispatch_command'],        # 19
        #                 )
        #             )


        self.accumulationBuffer[:]=[]


theStats = DispatcherStats()
