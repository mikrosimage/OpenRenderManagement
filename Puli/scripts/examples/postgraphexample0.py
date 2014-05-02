#!/usr/bin/python
# coding: utf-8
"""
Simple graph submission example.
- A simple task is created (and decomposed)
- A graph is created with the previously created task
- Classic submission
"""

from puliclient import Task, Graph
from optparse import OptionParser
import random

def process_args():
    usage = "Graph submission example"
    desc=""" """
    parser = OptionParser(usage=usage, description=desc, version="%prog 0.1" )
    parser.add_option("-n", "--name",       action="store", dest="jobname",     type=str,   default="toto",         help="")
    parser.add_option("-s", "--server",     action="store", dest="hostname",    type=str,   default="puliserver",   help="Specified a target host to send the request")
    parser.add_option("-p", "--port",       action="store", dest="port",        type=int,   default=8004,           help="Specified a target port")
    parser.add_option("-x", "--execute",    action="store_true", dest="execute",                                    help="Override submit param and executes job locally")
    parser.add_option("-d", "--display",    action="store_true", dest="dump",                                       help="Print graph json representation before process")

    parser.add_option("--lic",    	    action="store", dest="lic",    type=str,   default="" )
    parser.add_option("--min",              action="store", dest="min",    type=int,   default=20 )
    parser.add_option("--max",              action="store", dest="max",    type=int,   default=50 )
    parser.add_option("--num",              action="store", dest="num",    type=int,   default=10 )
    options, args = parser.parse_args()
    return options, args



if __name__ == '__main__':
    (options, args) = process_args()

    # # Randomize time spend in command
    # minRandom = -1*options.time*0.1
    # maxRandom = options.time*0.1
    # options.time = options.time + random.uniform(minRandom, maxRandom)

    #
    # Create custom graph
    #
    args =  { "args":"sleep `shuf -i 20-50 -n 1`", "start":1, "end":10, "packetSize":1 }
    tags =  { "prod":"test", "shot":"test", "nbFrames":10 }
    runner = "puliclient.contrib.commandlinerunner.CommandLineRunner"
    if options.lic != "":
        lic=options.lic
    else:
        lic=None

    simpleTask = Task( name="NoLic", arguments=args, tags=tags, runner=runner, lic=None )

    graph = Graph(options.jobname, poolName='default', tags=tags)
    tg = graph.addNewTaskGroup( name="TG")
    tg.addNewTask( name="Lic", arguments=args, tags=tags, runner=runner, lic=lic )

    tg2 = graph.addNewTaskGroup( name="TG2")
    tg2.addTask( simpleTask )

    if options.dump:
        print graph

    #
    # Execute
    #
    if options.execute:
        graph.execute()
    else:
        graph.submit(options.hostname, options.port)

