#!/usr/bin/python
# coding: utf-8


import sys
sys.path.insert(0,"/s/apps/lin/vfx_test_apps/OpenRenderManagement/Puli/src")


from puliclient import Task, Graph

tags =  { "prod":"test", "shot":"test" }

runner='puliclient.contrib.commandlinerunner.CommandLineRunner'
arguments={ 'args':'echo I AM RUNNIIIINNG !!!' }

graph = Graph('debug', tags=tags, poolName='default' )

graph.addNewTask( "DO_SOMETHING", tags=tags, arguments=arguments, runner=runner )

# graph.submit("puliserver", 8004)
graph.execute()
