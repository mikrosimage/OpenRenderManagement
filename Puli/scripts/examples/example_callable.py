#!/usr/bin/python
# coding: utf-8

import sys

from puliclient import Task, Graph, GraphError
from optparse import OptionParser

def process_args():
    usage = "Graph submission example"
    desc = """ """
    parser = OptionParser(usage=usage, description=desc, version="%prog 0.1")
    parser.add_option("-n", "--name",       action="store", dest="jobname",     type=str,   default="Example job", help="Name of the job")
    parser.add_option("-s", "--server",     action="store", dest="hostname",    type=str,   default="puliserver", help="Specified a target host to send the request")
    parser.add_option("-p", "--port",       action="store", dest="port",        type=int,   default=8004, help="Specified a target port")
    parser.add_option("-x", "--execute",    action="store_true", dest="execute", help="Override submit param and executes job locally")
    parser.add_option("-d", "--display",    action="store_true", dest="dump", help="Print graph json representation before process")

    parser.add_option("--num", action="store", dest="num", type=int, default=10, help="")

    options, args = parser.parse_args()
    return options, args

    pass

if __name__ == '__main__':
    (options, args) = process_args()

    tags = {"prod": "test", "shot": "test", "nbFrames": options.num}

    #
    # Create custom graph
    #
    from mymodule.submodule.farm import MyClass
    from mymodule.submodule.farm import myFunction

    try:
        graph = Graph(options.jobname, tags=tags)
        graph.addNewCallableTaskRAW(
            targetCall=myFunction,
            params={"param1": 1, "param2": tags}
        )

        graph.addNewCallableTaskRAW(
            targetCall=MyClass.myMethod,
            params={"param1": 1, "param2": 2, "param3": 3},
            tags={"nbFrames": 5, "prod": "tutu"},
            ramUse=32000
        )

        # graph.callOnFarm(MyClass.myMethod, "1er_arg", 2, param3="toto")

    except GraphError, e:
        print "oops an error occured during the graph creation: %s" % e
        sys.exit(0)

    if options.dump:
        print graph

    #
    # Execute
    #
    if options.execute:
        graph.execute()
    else:
        graph.submit(options.hostname, options.port)
