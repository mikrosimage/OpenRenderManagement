#!/usr/bin/python
# coding: utf-8
"""
A chained graph submission example.
- A graph is created with only a name attribute (a default taskgroup will be created).
- Graph's method addNewTask() will create a task and attach it to the graph
- A chain of dependencies is added to the graph, the default end status is [DONE]
    task1 > task4 > task3 > task2 > task5 > task6
- Classic submission
"""

from puliclient import Task, TaskGroup, Graph, DONE


if __name__ == '__main__':

    args =  { "cmd":"sleep 30", "start":1, "end":10, "packetSize":1 }
    tags =  { "prod":"test", "shot":"test" }
    decomposer = "puliclient.contrib.generic.GenericDecomposer"

    graph = Graph('simpleGraph', tags=tags)
    task1 = graph.addNewTask(name="task1", arguments=args, tags=tags, decomposer=decomposer)
    task2 = graph.addNewTask(name="task2", arguments=args, tags=tags, decomposer=decomposer)
    task3 = graph.addNewTask(name="task3", arguments=args, tags=tags, decomposer=decomposer)    
    task4 = graph.addNewTask(name="task4", arguments=args, tags=tags, decomposer=decomposer)    
    task5 = graph.addNewTask(name="task5", arguments=args, tags=tags, decomposer=decomposer)    
    task6 = graph.addNewTask(name="task6", arguments=args, tags=tags, decomposer=decomposer)    

    # Create a chain of dependencies, execution order will be: 
    # task1 > task4 > task3 > task2 > task5 > task6
    graph.addChain( [task1, task4, task3, task2, task5, task6] )

    graph.submit(host="pulitest")


# PREVIOUS METHOD (still valid)
    # task1 = Task(name="task1", arguments=args, decomposer=decomposer)
    # task2 = Task(name="task2", arguments=args, decomposer=decomposer)
    # task3 = Task(name="task3", arguments=args, decomposer=decomposer)
    # task4 = Task(name="task4", arguments=args, decomposer=decomposer)
    # task5 = Task(name="task5", arguments=args, decomposer=decomposer)
    # task6 = Task(name="task6", arguments=args, decomposer=decomposer)

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

# SIMPLIFIED GRAPH DUMP
# chain is task1 > task4 > task3 > task2 > task5 > task6
#
# {
#     "tasks": [
#         {
#             "tasks": [
#                 1, 
#                 2, 
#                 3, 
#                 4, 
#                 5, 
#                 6
#             ], 
#             "name": "simpleGraph", 
#             "type": "TaskGroup"
#         }, 
#         {
#             "name": "task1", 
#             "dependencies": [], 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task2", 
#             "dependencies": [
#                 [ 3, [ 3 ] ]
#             ], 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task3", 
#             "dependencies": [
#                 [ 4, [ 3 ] ]
#             ], 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task4", 
#             "dependencies": [
#                 [ 1, [ 3 ] ]
#             ], 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task5", 
#             "dependencies": [
#                 [ 2, [ 3 ] ]
#             ], 
#             "type": "Task", 
#         }, 
#         {
#             "name": "task6", 
#             "dependencies": [
#                 [ 5, [ 3 ] ]
#             ], 
#             "type": "Task", 
#         }
#     ], 
#     "name": "simpleGraph", 
#     "user": "jsa", 
#     "root": 0
# }
