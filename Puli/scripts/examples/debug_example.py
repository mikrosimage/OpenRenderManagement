#!/usr/bin/python
# coding: utf-8
from puliclient import Task, Graph


tags =  { "prod":"test", "shot":"test" }


task = Task(
                name="dis is whada want", 
                arguments={},
                tags=tags,
                runner="puliclient.contrib.puliDbg.sleep.DebugRunner",
                dispatchKey=5
            )

task.addCommand( 'ls', { 'cmd':'ls' } )
task.addCommand( 'sleep_10', { 'cmd':'sleep 10', 'start':2, 'end':5 } )


graph = Graph('debug', tags=tags, root=task, poolName='default' )
graph.submit("pulitest", 8004)