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
    
    defaultArgs = { "cmd":"sleep 5", "start":1, "end":1, "packetSize":1, "prod":"test", "shot":"test" }
    defaultDecomposer = "puliclient.contrib.generic.GenericDecomposer"

    graph = Graph('multi-level')

    tg1 = graph.addNewTaskGroup( name="taskgroup1" )
    tg1.addNewTask( name="task1.1", arguments=defaultArgs, decomposer=defaultDecomposer )
    tg1.addNewTask( name="task1.2", arguments=defaultArgs, decomposer=defaultDecomposer )
    tg1.addNewTask( name="task1.3", arguments=defaultArgs, decomposer=defaultDecomposer )

    tg2 = graph.addNewTaskGroup( name="taskgroup2" )
    tg2.addNewTask( name="task2.1", arguments=defaultArgs, decomposer=defaultDecomposer )
    tg2.addNewTask( name="task2.2", arguments=defaultArgs, decomposer=defaultDecomposer )

    print graph
    graph.submit("pulitest", 8004)

# PREVIOUS METHOD (still valid)
    # tg1 = TaskGroup( name = "tg1" )
    # sub1 = Task( name="t1.1", arguments=defaultArgs, decomposer=defaultDecomposer )
    # sub2 = Task( name="t1.2", arguments=defaultArgs, decomposer=defaultDecomposer )
    # sub3 = Task( name="t1.3", arguments=defaultArgs, decomposer=defaultDecomposer )
    # tg1.addTask(sub1)
    # tg1.addTask(sub2)
    # tg1.addTask(sub3)

    # tg2 = TaskGroup( name = "tg2" )
    # sub21 = Task( name="t2.1", arguments=defaultArgs, decomposer=defaultDecomposer )
    # sub22 = Task( name="t2.2", arguments=defaultArgs, decomposer=defaultDecomposer )
    # tg2.addTask(sub21)
    # tg2.addTask(sub22)

    # mainTG = TaskGroup( name = "multi-level" )
    # mainTG.addtaskGroup(tg1)
    # mainTG.addtaskGroup(tg2)
    # graph.add( mainTG )
