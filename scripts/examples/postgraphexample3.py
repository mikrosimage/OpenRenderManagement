#!/usr/bin/python
# coding: utf-8
"""
A diamond shaped graph example.

  A
 / \
B   C
 \ /
  D
"""

from puliclient import *

if __name__ == '__main__':
    
    args =  { "cmd":"sleep 10", "start":1, "end":2, "packetSize":1 }
    tags =  { "prod":"test", "shot":"test" }
    decomposer = "puliclient.contrib.generic.GenericDecomposer"

    # When creating a graph without a root task or taskgroup, a default taskgroup is created with the name of the graph
    graph = Graph('my job', tags=tags)

    #
    # Several ways to create and add nodes
    #

    # Create nodes detached from the graph
    task1 = Task(name="task1", arguments=args, tags=tags, decomposer=decomposer)
    task2 = Task(name="task2", arguments=args, tags=tags, decomposer=decomposer)
    task3 = Task(name="task3", arguments=args, tags=tags, decomposer=decomposer)

    # and add them in a list
    graph.addList( [task1, task2] )

    # Or elem by elem
    graph.add( task3 )

    # Or add elem directly in the graph
    anotherTask = graph.addNewTask( name="another task",
                      arguments=args,
                      tags=tags,
                      decomposer=decomposer )

    # Create complex dependencies like a diamond shaped graph 
    # NB: default end status is [DONE]
    graph.addEdges( [
            (task1, task2),
            (task1, task3),
            (task2, anotherTask),
            (task3, anotherTask)
            ] )

    # graph.submit("pulitest", 8004)
    graph.execute()

# PREVIOUS METHOD (still valid)
	# task1 = Task(name="task1", arguments=args, decomposer=decomposer)
	# task2 = Task(name="task2", arguments=args, decomposer=decomposer)
	# task3 = Task(name="task3", arguments=args, decomposer=decomposer)
	# anotherTask = Task(name="anotherTask", arguments=args, decomposer=decomposer)

	# mainTG = TaskGroup( name="my job" )
	# mainTG.addTask(task1)
	# mainTG.addTask(task2)
	# mainTG.addTask(task3)
	# mainTG.addTask(anotherTask)

	# task2.dependsOn( task1, [DONE] )
	# task3.dependsOn( task1, [DONE] )
	# anotherTask.dependsOn( task2, [DONE] )
	# anotherTask.dependsOn( task3, [DONE] )

	# graph = Graph('toto', mainTG) # Did you know the name of the graph was never used ? :)

	# graph.submit("pulitest", 8004, )

# SIMPLIFIED GRAPH DUMP
# {
#     "tasks": [
#         {
#             "tasks": [
#                 1, 
#                 2, 
#                 3, 
#                 4
#             ], 
#             "name": "my job", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "task1", 
#             "dependencies": [], 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task2", 
#             "dependencies": [
#                 [ 1, [3] ]
#             ], 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task3", 
#             "dependencies": [
#                 [ 1, [3] ]
#             ], 
#             "type": "Task", 
#         }, 
#         {
#             "name": "another task", 
#             "dependencies": [
#                 [ 2, [3]], 
#                 [ 3, [3]]
#             ], 
#             "type": "Task", 
#         }
#     ], 
#     "name": "my job", 
#     "user": "jsa", 
#     "root": 0
# }
