#!/usr/bin/python
# coding: utf-8
from puliclient import Task, Graph

###
### NB erreur a tester quand on utilise des parametres qu'il en manque un a l'import

tags =  { "prod":"test", "shot":"test" }


task = Task(
                name="custom_task", 
                arguments={},
                tags=tags,
                runner="puliclient.contrib.puliDbg.sleep.DebugRunner",
                dispatchKey=5
            )


task.addCommand( 'ls', { 'cmd':'ls', 'floatParam':0.1 } )
# task.addCommand( 'sleep_10', { 'cmd':'sleep 10', 'start':2, 'end':5 } )



graph = Graph('debug', tags=tags, root=task, poolName='default' )
graph.submit("pulitest", 8004)