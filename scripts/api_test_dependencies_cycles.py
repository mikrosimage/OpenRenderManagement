# encoding: UTF-8
from puliclient import TaskGroup, Task, Graph, DONE

A = TaskGroup("A")
B = Task("B", "")
C = Task("C", "")
D = Task("D", "")
E = Task("E", "")

A.addTask(B)
A.addTask(C)
A.addTask(D)
A.addTask(E)

# set up dependencies
B.dependsOn(E, [DONE])
C.dependsOn(B, [DONE])
D.dependsOn(C, [DONE])
E.dependsOn(D, [DONE])

g = Graph("Graph", A)
g.submit("", 8004)
