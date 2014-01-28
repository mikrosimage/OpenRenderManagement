#!/usr/bin/python
# coding: utf-8
from puliclient import Task, Graph


tags =  { "prod":"test", "shot":"test" }
arguments={ 'args':'echo toto', 'delay':2, 'start':1, 'end':2, 'packetSize':1}
# decomposer='puliclient.contrib.generic.GenericDecomposer'
runner='puliclient.contrib.commandlinerunner.CommandLineRunner'
# runner='puliclient.contrib.puliDbg.sleep.DebugRunner'
# runner='puliclient.contrib.generic.GenericRunner'
# task = Task(
#                 name="custom_task", 
#                 arguments={ 'cmd':'ls', 'delay':5, 'start':1, 'end':10 , 'packetSize':1},
#                 tags=tags,
#                 runner="puliclient.contrib.puliDbg.sleep.DebugRunner",
#                 decomposer="puliclient.contrib.generic.GenericDecomposer",
#                 dispatchKey=5
#             )

# task.addCommand( 'sleep_10', { 'cmd':'sleep 10', 'start':2, 'end':5 } )
graph = Graph('debug', tags=tags, poolName='default' )

# task1 = graph.addNewTask( "TASK_1", tags=tags, arguments=arguments, runner=runner, decomposer=decomposer )
# task2 = graph.addNewTask( "TASK_2", tags=tags, arguments=arguments, runner=runner, decomposer=decomposer )
task1 = graph.addNewTask( "TASK_1", tags=tags, arguments=arguments, runner=runner )
task2 = graph.addNewTask( "TASK_2", tags=tags, arguments=arguments, runner=runner )
task1.dependsOn(task2)

# graph.submit("pulitest", 8004)
graph.execute()
