#!/usr/bin/python
# coding: utf-8

import sys
import time

from puliclient import Task, Graph, GraphError
from optparse import OptionParser

from mymodule.submodule.farm import MyClass


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

    tags = {"prod": "zaza", "shot": "zuzu", "nbFrames": options.num}

    #
    # Create custom graph
    #
    try:
        graph = Graph(options.jobname, tags=tags)


        # graph.addNewCallableTaskRAW(
        #     targetCall=myFunction,
        #     name="une_fonction",
        #     user_args=(1, tags)
        # )

        # command = "sleep `shuf -i 20-30 -n 1`"
        # args = {"args": command, "start": 1, "end": 10, "packetSize": 1}
        # runner = "puliclient.contrib.commandlinerunner.CommandLineRunner"
        # graph.addNewTask(name="Timer", arguments=args, tags=tags, runner=runner, timer=time.time()+600)

        graph.addNewCallable(
            MyClass.myMethod,
            "une_methode",
            user_args=("toto", 2),
            user_kwargs={"param3": 3},
            tags=tags,
            ramUse=4000
        )

        tg = graph.addNewTaskGroup("group")

        from mymodule.submodule.farm import myFunction
        tg.addNewCallable(myFunction, "une_fonction", user_args=[True,"any text"], tags=tags)

        # graph.addCallable(MyClass.myMethod, "1er_arg", 2, param3='test', name="zozo", tags=tags, timer=time.time()+600)

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
