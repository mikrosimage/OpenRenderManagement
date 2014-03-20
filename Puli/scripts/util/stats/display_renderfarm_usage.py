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

import simplejson as json

###########################################################################################################################
# {
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


if __name__ == "__main__":
    print "start..."

    nbjobs=[]
    nb_running_commands=[]
    with open("/s/prods/ddd/_sandbox/jsa/stats/usage_good.log") as f:
        # data = f.read()
        fullLog = json.load(f)
        for data in fullLog:
            # print data['jobs']['total']
            # print data['commands']
            nbjobs.append(data['jobs']['total'])
            nb_running_commands.append(data['commands']['RUNNING'])
            # nb_blocked_commands.append(data['commands']['BLOCKED'])
            # nb_ready_commands.append(data['commands']['READY'])

    print "nb events: %d" % len(nb_running_commands)
    print "done..."


    usage = pygal.Line( x_label_rotation=40, logarithmic=False )
    usage.title = 'Usage'

    # usage.add('nb jobs', nbjobs )
    usage.add('nb running commands', nb_running_commands )

    usage.render_to_file( os.path.join("/s/prods/ddd/_sandbox/jsa/stats", "usage.svg") )
