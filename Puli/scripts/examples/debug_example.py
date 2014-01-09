#!/usr/bin/python
# coding: utf-8
from puliclient import Task, Graph


args =  { "cmd":"sleep 10", "start":1, "end":2, "packetSize":1 }
tags =  { "prod":"test", "shot":"test", "nbFrames":2 }
decomposer = "puliclient.contrib.generic.GenericDecomposer"

graph = Graph('debug', tags=tags)

tg1 = graph.addNewTaskGroup( name="taskgroup1", tags=tags )
tg1.addNewTask( name="task11", arguments=args, decomposer=decomposer )
tg1.addNewTask( name="task12", arguments=args, decomposer=decomposer )

tg2 = graph.addNewTaskGroup( name="taskgroup2", tags=tags )
tg21 = tg2.addNewTaskGroup( name="taskgroup21", tags=tags )
tg21.addNewTask( name="task211", arguments=args, tags=tags, decomposer=decomposer )
tg21.addNewTask( name="task212", arguments=args, tags=tags, decomposer=decomposer )

# print graph
graph.submit("pulitest", 8004)