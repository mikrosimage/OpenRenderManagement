#!/usr/bin/python
# coding: utf-8
from puliclient import Task, Graph


args =  { "cmd":"sleep 10", "start":1, "end":200, "packetSize":1 }
tags =  { "prod":"test", "shot":"test", "nbFrames":200 }
decomposer = "puliclient.contrib.generic.GenericDecomposer"

# import pudb;pu.db
task = Task( name="task", arguments=args, tags=tags, runner="puliclient.contrib.generic.GenericRunner", dispatchKey=-5 )
task.addCommand( 'i am sleepy', args )

graph = Graph('debug', tags=tags, root=task )

graph.submit("pulitest", 8004)