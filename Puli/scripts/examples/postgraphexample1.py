#!/usr/bin/python
# coding: utf-8
"""
Simple graph submission example.
- A graph is created with only a name attribute (a default taskgroup will be created).
- Graph's method addNewTask() will create a task, decompose it and attach it to the graph
- Classic submission
"""

from puliclient import Task, Graph, GraphDumper

if __name__ == '__main__':

    args =  { "cmd":"sleep 30", "start":1, "end":10, "packetSize":1 }
    tags =  { "prod":"test", "shot":"test" }
    decomposer = "puliclient.contrib.generic.GenericDecomposer"

    # When creating a graph without a root task or taskgroup, a default taskgroup is created with the name of the graph
    graph = Graph('simpleGraph', poolName='default', tags=tags)

    # Each task will be decomposed an have its resulting commands attached
    graph.addNewTask( name="first", arguments=args, tags=tags, decomposer=decomposer )
    graph.addNewTask( name="second", arguments=args, tags=tags, decomposer=decomposer )
    graph.addNewTask( name="third", arguments=args, tags=tags, decomposer=decomposer )

    graph.submit("pulitest", 8004)

# PREVIOUS METHOD (still valid)
#
# tags =  { "prod":"test", "shot":"test" }

# simpleTask = Task(name="simpleTask",
#                   arguments={ "cmd":"sleep 10", "start":1, "end":100, "packetSize":5, "prod":"test", "shot":"test" },
#                   decomposer="puliclient.contrib.generic.GenericDecomposer")
# graph = Graph('simpleGraph', simpleTask, tags=tags)
# graph.submit("pulitest", 8004, )



# SIMPLIFIED GRAPH DUMP:
# {
#     "tasks": [
#         {
#             "tasks": [
#                 1, 
#                 2, 
#                 3
#             ], 
#             "name": "simpleGraph", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "first", 
#             "type": "Task", 
#         }, 
#         {
#             "name": "second", 
#             "type": "Task", 
#         }, 
#         {
#             "name": "third", 
#             "type": "Task", 
#         }
#     ], 
#     "name": "simpleGraph", 
#     "meta": {}, 
#     "user": "jsa", 
#     "root": 0
# }

