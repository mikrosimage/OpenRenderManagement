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

def process_args():
    usage = "Graph submission example"
    desc=""" """
    parser = OptionParser(usage=usage, description=desc, version="%prog 0.1" )
    parser.add_option("-n", "--name",       action="store", dest="jobname",     type=str,   default="toto",         help="")
    parser.add_option("-s", "--server",     action="store", dest="hostname",    type=str,   default="puliserver",   help="Specified a target host to send the request")
    parser.add_option("-p", "--port",       action="store", dest="port",        type=int,   default=8004,           help="Specified a target port")
    parser.add_option("-x", "--execute",    action="store_true", dest="execute",                                    help="Override submit param and executes job locally")
    parser.add_option("-d", "--display",    action="store_true", dest="dump",                                       help="Print graph json representation before process")
    options, args = parser.parse_args()
    return options, args



if __name__ == '__main__':
    (options, args) = process_args()


    #
    # Create custom graph
    #
    args =  { "cmd":"sleep 30", "start":1, "end":10, "packetSize":1 }
    tags =  { "prod":"test", "shot":"test", "nbFrames":10 }
    decomposer = "puliclient.contrib.generic.GenericDecomposer"

    simpleTask = Task( name=options.jobname, arguments=args, tags=tags, decomposer=decomposer )

    graph = Graph(options.jobname, poolName='default', tags=tags, root=simpleTask)

    if options.dump:
        print graph

    #
    # Execute
    #
    if options.execute:
        graph.execute()
    else:
        graph.submit(options.hostname, options.port)

