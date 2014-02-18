#!/usr/bin/python
# coding: utf-8
"""
Simple graph submission example.
- A simple task is created (and decomposed)
- A graph is created with the previously created task
- Classic submission
"""

from puliclient import Task, Graph, GraphDumper

if __name__ == '__main__':

    args =  { "cmd":"sleep 20", "start":1, "end":100, "packetSize":1 }
    tags =  { "prod":"test", "shot":"test", "nbFrames":100 }
    decomposer = "puliclient.contrib.generic.GenericDecomposer"

    simpleTask = Task( name="todo", arguments=args, tags=tags, decomposer=decomposer )

    graph = Graph('simpleGraph', poolName='DDD_katana', tags=tags, root=simpleTask)
    graph.submit("puliserver", 8004)
