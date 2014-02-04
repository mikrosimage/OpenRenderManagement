#!/usr/bin/python
# coding: utf-8

import sys
sys.path.insert(0,"/s/apps/lin/vfx_test_apps/OpenRenderManagement/Puli/src")


from puliclient import Task, Graph

# command = "nuke -x -F %%MI_FRAME%% ma_comp.nk"
# command = "nuke -x -F %%MI_START%%-%%MI_END%% ma_comp.nk"
# command = "echo currFrame=%%MI_FRAME%% in [%%MI_START%%-%%MI_END%%]"

command = "sleep %%MI_FRAME%%s"
arguments={ 'cmd':command, 'start':1, 'end':50 , 'packetSize':10}

tags =  { "prod":"test", "shot":"test" }

graph = Graph('Testing DefaultRunner', tags=tags, poolName='default' )
task1 = graph.addNewTask( "TASK_1", tags=tags, arguments=arguments )
graph.submit( "puliserver", 8004)

#
# Pour une execution en local
#
#graph.execute()