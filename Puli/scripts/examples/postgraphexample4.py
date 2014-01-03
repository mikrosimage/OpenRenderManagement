#!/usr/bin/python
# coding: utf-8
"""
A multi-level graph example.

multi-level
  |-taskgroup1
  |   |-task1.1
  |   |-task1.2
  |   `-task1.3
  `-taskgroup2
      |-task2.1
      `-task2.2
"""

from puliclient import *

if __name__ == '__main__':
    
    args =  { "cmd":"sleep 30", "start":1, "end":10, "packetSize":1 }
    tags =  { "prod":"test", "shot":"test", "nbFrames":10 }
    decomposer = "puliclient.contrib.generic.GenericDecomposer"

    graph = Graph('multi-level', tags=tags)

    tg1 = graph.addNewTaskGroup( name="taskgroup1", tags=tags )
    tg1.addNewTask( name="task1.1", arguments=args, decomposer=decomposer )
    tg1.addNewTask( name="task1.2", arguments=args, decomposer=decomposer )
    tg1.addNewTask( name="task1.3", arguments=args, decomposer=decomposer )

    # tg2 = graph.addNewTaskGroup( name="taskgroup2", tags=tags )
    # tg2.addNewTask( name="task2.1", arguments=args, tags=tags, decomposer=decomposer )
    # tg2.addNewTask( name="task2.2", arguments=args, tags=tags, decomposer=decomposer )

    # print graph
    graph.submit("pulitest", 8004)

# PREVIOUS METHOD (still valid)
    # tg1 = TaskGroup( name = "tg1" )
    # sub1 = Task( name="t1.1", arguments=args, decomposer=decomposer )
    # sub2 = Task( name="t1.2", arguments=args, decomposer=decomposer )
    # sub3 = Task( name="t1.3", arguments=args, decomposer=decomposer )
    # tg1.addTask(sub1)
    # tg1.addTask(sub2)
    # tg1.addTask(sub3)

    # tg2 = TaskGroup( name = "tg2" )
    # sub21 = Task( name="t2.1", arguments=args, decomposer=decomposer )
    # sub22 = Task( name="t2.2", arguments=args, decomposer=decomposer )
    # tg2.addTask(sub21)
    # tg2.addTask(sub22)

    # mainTG = TaskGroup( name = "multi-level" )
    # mainTG.addtaskGroup(tg1)
    # mainTG.addtaskGroup(tg2)
    # graph.add( mainTG )

# SIMPLIFIED GRAPH DUMP
# {
#     "tasks": [
#         {
#             "tasks": [
#                 1, 
#                 5
#             ], 
#             "name": "multi-level", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "tasks": [
#                 2, 
#                 3, 
#                 4
#             ], 
#             "name": "taskgroup1", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "task1.1", 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task1.2", 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task1.3", 
#             "type": "Task", 
#         }, 
#         {
#             "tasks": [
#                 6, 
#                 7
#             ], 
#             "name": "taskgroup2", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "task2.1", 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task2.2", 
#             "type": "Task", 
#         }
#     ], 
#     "name": "multi-level", 
#     "user": "jsa", 
#     "root": 0
# }
  
