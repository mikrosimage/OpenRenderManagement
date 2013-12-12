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
    
    defaultArguments = { "cmd":"sleep 5", "start":1, "end":1, "packetSize":1, "prod":"test", "shot":"test" }
    defaultDecomposer = "puliclient.contrib.generic.GenericDecomposer"

    # When creating a graph without a root task or taskgroup, a default taskgroup is created with the name of the graph
    graph = Graph('my job')

    #
    # Several ways to create and add nodes
    #

    # Create nodes detached from the graph
    task1 = Task(name="task1", arguments=defaultArguments, decomposer=defaultDecomposer)
    task2 = Task(name="task2", arguments=defaultArguments, decomposer=defaultDecomposer)
    task3 = Task(name="task3", arguments=defaultArguments, decomposer=defaultDecomposer)

    # and add them in a list
    graph.addList( [task1, task2] )

    # Or elem by elem
    graph.add( task3 )

    # Or add elem directly in the graph
    anotherTask = graph.addNewTask( name="another task",
                      arguments={ "cmd":"sleep 10", "start":1, "end":1, "packetSize":1, "prod":"test", "shot":"test" },
                      decomposer=defaultDecomposer )

    # Create complex dependencies like a diamond shaped graph 
    # NB: default end status is [DONE]
    graph.addEdges( [
            (task1, task2),
            (task1, task3),
            (task2, anotherTask),
            (task3, anotherTask)
            ] )

    graph.submit("pulitest", 8004)


# PREVIOUS METHOD (still valid)
	# task1 = Task(name="task1", arguments=defaultArguments, decomposer=defaultDecomposer)
	# task2 = Task(name="task2", arguments=defaultArguments, decomposer=defaultDecomposer)
	# task3 = Task(name="task3", arguments=defaultArguments, decomposer=defaultDecomposer)
	# anotherTask = Task(name="anotherTask", arguments=defaultArguments, decomposer=defaultDecomposer)

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
