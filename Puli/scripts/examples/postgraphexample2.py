#!/usr/bin/python
# coding: utf-8
"""
A chained graph submission example.
- A graph is created with only a name attribute (a default taskgroup will be created).
- Graph's method addNewTask() will create a task and attach it to the graph
- A chain of dependencies is added to the graph, the default end status is [DONE]
- Classic submission
"""

from puliclient import Task, TaskGroup, Graph, DONE


if __name__ == '__main__':

    defaultArgs =  { "cmd":"sleep 5", "start":1, "end":1, "packetSize":1, "prod":"test", "shot":"test" }
    defaultDecomposer = "puliclient.contrib.generic.GenericDecomposer"

    graph = Graph('simpleGraph')
    task1 = graph.addNewTask(name="task1", arguments=defaultArgs, decomposer=defaultDecomposer)
    task2 = graph.addNewTask(name="task2", arguments=defaultArgs, decomposer=defaultDecomposer)
    task3 = graph.addNewTask(name="task3", arguments=defaultArgs, decomposer=defaultDecomposer)    
    task4 = graph.addNewTask(name="task4", arguments=defaultArgs, decomposer=defaultDecomposer)    
    task5 = graph.addNewTask(name="task5", arguments=defaultArgs, decomposer=defaultDecomposer)    
    task6 = graph.addNewTask(name="task6", arguments=defaultArgs, decomposer=defaultDecomposer)    

    # Create a chain of dependencies, execution order will be: 
    # task1 > task4 > task3 > task2 > task5 > task6
    graph.addChain( [task1, task4, task3, task2, task5, task6] )

    graph.submit(host="pulitest")


# PREVIOUS METHOD (still valid)
    # task1 = Task(name="task1", arguments=defaultArgs, decomposer=defaultDecomposer)
    # task2 = Task(name="task2", arguments=defaultArgs, decomposer=defaultDecomposer)
    # task3 = Task(name="task3", arguments=defaultArgs, decomposer=defaultDecomposer)
    # task4 = Task(name="task4", arguments=defaultArgs, decomposer=defaultDecomposer)
    # task5 = Task(name="task5", arguments=defaultArgs, decomposer=defaultDecomposer)
    # task6 = Task(name="task6", arguments=defaultArgs, decomposer=defaultDecomposer)

    # mainTG = TaskGroup( name="group" )
    # mainTG.addTask(task1)
    # mainTG.addTask(task2)
    # mainTG.addTask(task3)
    # mainTG.addTask(task4)
    # mainTG.addTask(task5)
    # mainTG.addTask(task6)
    
    # task4.dependsOn( task1, [DONE] )
    # task3.dependsOn( task4, [DONE] )
    # task2.dependsOn( task3, [DONE] )
    # task5.dependsOn( task2, [DONE] )
    # task6.dependsOn( task5, [DONE] )

    # graph = Graph('simpleGraph', mainTG)
    # graph.submit("pulitest", 8004)
