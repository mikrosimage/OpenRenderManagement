#!/usr/bin/python2.6
# -*- coding: utf8 -*-

"""
name: config.py

Module holding param values that might be reloaded without restarting the dispatcher
The dispatcher handles a "reconfig" request wich ask the worker application to reload the source config file and recreate the conf object

Basic usage:

    import theStats
    singletonconfig.load( settings.CONFDIR + "/config.ini" )

    # Access the options with module's get() method
    print "one_value = " + singletonconfig.get('ONE_SECTION','ONE_FIELD')

    # Or with direct access to the key/value dict for faster access
    print "one_value = " + singletonconfig.conf.['ONE_SECTION']['ONE_FIELD']

"""
__author__      = "Jérôme Samson"
__copyright__   = "Copyright 2014, Mikros Image"

import logging
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
# It is at DEBUG level to avoid flooding the main log in production env
#
LOGGER = logging.getLogger('stats')

class DispatcherStats():
    #
    # Class holding custom infos on the dispatcher.
    # This data can be periodically flushed in a specific log file for later use
    #

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

        cycleData = [ self.cycleDate, copy( self.cycleTimers ), copy(self.cycleCounts) ]
        self.accumulationBuffer.append( cycleData )

        # Clean data for next cycle (only counts need to be cleaned, timer are overwritten)
        self._resetCounts()

        # Dump to file
        if singletonconfig.get('CORE','STATS_BUFFER_SIZE') < len(self.accumulationBuffer):
            self._flush()


        return True

    def _flush( self ):
        """
        TODO flush in a dedicated thread
        """
        for line in self.accumulationBuffer:
            # print json.dumps( line )

            # LOGGER.debug( 
            #     "%f;%f;%f;%f;%f;%f;%f;%f;%f;%d;%d;%d;%d;%d;%d;%d;%d;%d" % ( 
            #     line[0], 

            #     line[1]['update_tree'],         # from dispatchLoop
            #     line[1]['update_rn'],           # from dispatchLoop
            #     line[1]['update_dependencies'], # from dispatchLoop
            #     line[1]['update_db'],           # from dispatchLoop
            #     line[1]['compute_assignment'],  # from dispatchLoop
            #     line[1]['send_assignment'],     # from dispatchLoop
            #     line[1]['release_finishing'],   # from dispatchLoop
            #     line[1]['time_elapsed'],        # from dispatchLoop

            #     line[2]['incoming_requests'],   # from base ressource handler
            #     line[2]['incoming_get'],        # from base ressource handler
            #     line[2]['incoming_post'],       # from base ressource handler
            #     line[2]['incoming_put'],        # from base ressource handler
            #     line[2]['incoming_delete'],     # from base ressource handler

            #     line[2]['add_graphs'],          # from WS add graph
            #     line[2]['add_rns'],             # from WS rendernodes
            #     line[2]['update_commands'],     # from WS rendernodes
            #     line[2]['num_assignments'],     # from dispatchLoop
            #     )
            # )

            with open( os.path.join(settings.LOGDIR, "stats.log") , 'a') as f:

                f.write(
                    "%f;%f;%f;%f;%f;%f;%f;%f;%f;%d;%d;%d;%d;%d;%d;%d;%d;%d\n" % ( 
                        line[0], 

                        line[1]['update_tree'], 
                        line[1]['update_rn'], 
                        line[1]['update_dependencies'], 
                        line[1]['update_db'], 
                        line[1]['compute_assignment'], 
                        line[1]['send_assignment'], 
                        line[1]['release_finishing'], 
                        line[1]['time_elapsed'], 

                        line[2]['incoming_requests'], 
                        line[2]['incoming_get'], 
                        line[2]['incoming_post'], 
                        line[2]['incoming_put'], 
                        line[2]['incoming_delete'], 

                        line[2]['add_graphs'], 
                        line[2]['add_rns'], 
                        line[2]['update_commands'], 
                        line[2]['num_assignments'],     # from dispatchLoop
                        )
                    )

        self.accumulationBuffer[:]=[]


theStats = DispatcherStats()
