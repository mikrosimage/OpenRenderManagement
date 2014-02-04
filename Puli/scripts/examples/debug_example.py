#!/usr/bin/python
# coding: utf-8

import sys
sys.path.insert(0,"/s/apps/lin/vfx_test_apps/OpenRenderManagement/Puli/src")


from puliclient import Task, Graph

# command = "nuke -x -F %%MI_FRAME%% ma_comp.nk"
# command = "nuke -x -F %%MI_START%%-%%MI_END%% ma_comp.nk"

# command = "echo currFrame=%%MI_FRAME%% in [%%MI_START%%-%%MI_END%%]"
# command = "echo \"comId=$PULI_COMMAND_ID task=$PULI_TASK_NAME log=$PULI_LOG\""
command = "env | grep ^PULI"

arguments={ 'args':command, 'start':1, 'end':2 , 'packetSize':1}
tags =  { "prod":"test", "shot":"test" }

graph = Graph('debug', tags=tags, poolName='default' )
task1 = graph.addNewTask( "TASK_1", tags=tags, arguments=arguments, runner="puliclient.contrib.commandlinerunner.CommandLineRunner" )
graph.submit( "pulitest", 8004)
#graph.execute()