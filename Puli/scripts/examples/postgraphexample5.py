#!/usr/bin/python
# coding: utf-8
"""
Graph with dependencies between a task and a taskgroup.
1. a task waits the end of a taskgroup --> ok
2. a taskgroup waits the end of a task --> ok
3. a taskgroup waits the end of a taskgroup --> ok
"""

from puliclient import Task, Graph, GraphDumper

if __name__ == '__main__':
    args =  { "cmd":"sleep 30", "start":1, "end":10, "packetSize":1 }
    tags =  { "prod":"test", "shot":"test" }
    decomposer = "puliclient.contrib.generic.GenericDecomposer"

	#
    # 1. a task waits the end of a taskgroup
    #
    graph = Graph('1st case', tags=tags)
    tg1 = graph.addNewTaskGroup( name="TG1" )
    tg1.addNewTask( name="task1", arguments=args, tags=tags, decomposer=decomposer )
    tg1.addNewTask( name="task2", arguments=args, tags=tags, decomposer=decomposer )
    task3 = graph.addNewTask( name="task3", arguments=args, tags=tags, decomposer=decomposer )
    graph.addEdges( [(tg1, task3)] )
    graph.submit("pulitest", 8004)

    #
    # 2. a taskgroup waits the end of a task
    #
    graph = Graph('2nd case', tags=tags)
    tg1 = graph.addNewTaskGroup( name="TG1" )
    tg1.addNewTask( name="task1", arguments=args, tags=tags, decomposer=decomposer )
    tg1.addNewTask( name="task2", arguments=args, tags=tags, decomposer=decomposer )

    task3 = graph.addNewTask( name="task3", arguments=args, tags=tags, decomposer=decomposer )
    task4 = graph.addNewTask( name="task4", arguments=args, tags=tags, decomposer=decomposer )
    graph.addEdges( [(task3, tg1),
                    (task4, tg1)] )
    graph.submit("pulitest", 8004)

    #
    # 3. a taskgroup waits the end of another taskgroup
    #
    graph = Graph('3rd case', tags=tags)
    tg1 = graph.addNewTaskGroup( name="TG1" )
    tg1.addNewTask( name="task1", arguments=args, tags=tags, decomposer=decomposer )
    tg1.addNewTask( name="task2", arguments=args, tags=tags, decomposer=decomposer )

    tg2 = graph.addNewTaskGroup( name="TG2" )
    tg2.addNewTask( name="task3", arguments=args, tags=tags, decomposer=decomposer )
    tg2.addNewTask( name="task4", arguments=args, tags=tags, decomposer=decomposer )

    graph.addEdges( [(tg2, tg1)] )
    graph.submit("pulitest", 8004)



# SIMPLIFIED GRAPH DUMP:
# {
#     "tasks": [
#         {
#             "tasks": [
#                 1, 
#                 2
#             ], 
#             "name": "simpleGraph", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "task3", 
#             "dependencies": [
#                 [ 2, [3] ]
#             ], 
#             "type": "Task", 
#         }, 
#         {
#             "tasks": [
#                 3, 
#                 4
#             ], 
#             "name": "TG1", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "task1", 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task2", 
#             "type": "Task", 
#         }
#     ], 
#     "name": "simpleGraph", 
#     "user": "jsa", 
#     "root": 0 
# }



# BEFORE 

# {
#     "tasks": [
#         {
#             "tasks": [
#                 1, 
#                 2
#             ], 
#             "dependencies": [], 
#             "name": "2nd case", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "task3", 
#             "dependencies": [], 
#             "type": "Task", 
#         }, 
#         {
#             "tasks": [
#                 3, 
#                 4
#             ], 
#             "dependencies": [
#                 [ 1, [3] ]
#             ], 
#             "name": "TG1", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "task1", 
#             "dependencies": [], 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task2", 
#             "dependencies": [], 
#             "type": "Task", 
#         }
#     ], 
#     "name": "2nd case", 
#     "root": 0
# }

# AFTER
# {
#     "tasks": [
#         {
#             "tasks": [
#                 1, 
#                 2
#             ], 
#             "dependencies": [], 
#             "name": "2nd case", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "task3", 
#             "type": "Task", 
#         }, 
#         {
#             "tasks": [
#                 3, 
#                 4
#             ], 
#             "dependencies": [
#                 [1, [3]]
#             ], 
#             "name": "TG1", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "task1", 
#             "dependencies": [
#                 [1, [3]]
#             ], 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task2", 
#             "dependencies": [
#                 [1, [3]]
#             ], 
#             "type": "Task", 
#         }
#     ], 
#     "name": "2nd case", 
#     "root": 0
# }







# AFTER WITH 2 DEPENDENCIES

# {
#     "tasks": [
#         {
#             "tasks": [
#                 1, 
#                 2, 
#                 3
#             ], 
#             "name": "2nd case", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "task3", 
#             "dependencies": [], 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task4", 
#             "dependencies": [], 
#             "type": "Task", 
#         }, 
#         {
#             "tasks": [
#                 4, 
#                 5
#             ], 
#             "dependencies": [
#                 [1, [3]], 
#                 [2, [3]]
#             ], 
#             "name": "TG1", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "task1", 
#             "dependencies": [
#                 [1, [3]], 
#                 [2, [3]]
#             ], 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task2", 
#             "dependencies": [
#                 [1, [3]], 
#                 [2, [3]]
#             ], 
#             "type": "Task", 
#         }
#     ], 
#     "name": "2nd case", 
#     "root": 0
# }
