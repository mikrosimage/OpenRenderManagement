#!/usr/bin/python
# coding: utf-8

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
    cmdArgs =  { "cmd":"sleep 20", "start":1, "end":20, "packetSize":1 }
    tags =  { "prod":"test", "shot":"test" }

    simpleTask = Task( name="todo", arguments=cmdArgs, tags=tags, priority=10, lic="ocula" )
    graph = Graph(options.jobname, poolName='default', tags=tags, root=simpleTask)


#    graph = Graph(options.jobname, poolName='default', tags=tags)
#
#    t1 = graph.addNewTask( name="todo1", arguments=cmdArgs, tags=tags, runner='puliclient.contrib.generic.GenericRunner' )
#    t2 = graph.addNewTask( name="todo2", arguments=cmdArgs, tags=tags, runner='puliclient.contrib.generic.GenericRunner' )
#    t3 = graph.addNewTask( name="todo3", arguments=cmdArgs, tags=tags, runner='puliclient.contrib.generic.GenericRunner' )
#
#    graph.addChain( [ t1, t2, t3 ] )
#
#    tg = graph.addNewTaskGroup( name="mongroup" )
#    tg1 = tg.addNewTask( name="tg1", arguments=cmdArgs, tags=tags, runner='puliclient.contrib.generic.GenericRunner' )
#
#    graph.addEdges( [ (tg, t1 ) ] )

    if options.dump:
        print graph

    #
    # Execute
    #
    if options.execute:
        graph.execute()
    else:
        graph.submit(options.hostname, options.port)

