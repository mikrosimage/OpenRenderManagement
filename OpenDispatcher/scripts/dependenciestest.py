# encoding: UTF-8
from octopus.client import *
from octopus.core.enums.node import *
import urlparse

def sendtask(taskName):

    container = Task(name=taskName,
                     jobtype='',
                     arguments={},
                     dependencies={},
                     subtasks=[],
                     maxrn=1,
                     validator='0')

    task = Task(name='testtask',
                jobtype='octopus.core.jobtypes.printinfile.PrintInFile',
                arguments={ 'text': 'plop' },
                dependencies={},
                subtasks=[],
                maxrn=1,
                validator='0',
                requirements={ "player2k": "yes", "someval": ["a", "f"] })
#                requirements = {}  )
    dependingTasks = []
    for i in xrange(1, 10):
        dependingTasks.append(Task(name='testtask%d' % i,
                              jobtype='octopus.core.jobtypes.printinfile.PrintInFile',
                              arguments={ 'text': 'plop' },
                              dependencies={task: [NODE_DONE, NODE_RUNNING]},
                              subtasks=[],
                              maxrn=1,
                              validator='0',
                              requirements={}))

#    task.decompose()

    container.subtasks.append(task)

    subcontainer = Task(name='subtask',
                        jobtype='',
                        arguments={},
                        dependencies={},
                        subtasks=dependingTasks,
                        maxrn=1,
                        validator='0',
#                        requirements = { "player2k": "yes", "someval": ["a", "f"] }  )
                        requirements={})



    finalTask = Task(name='finalTask',
                jobtype='octopus.core.jobtypes.printinfile.PrintInFile',
                arguments={ 'text': 'plop' },
                dependencies={},
                subtasks=[],
                maxrn=1,
                validator='0',
                requirements={})

    folderTwo = Task(name='waiting for container',
                        jobtype='',
                        arguments={},
                        dependencies={subcontainer: [NODE_DONE]},
                        subtasks=[finalTask],
                        maxrn=1,
                        validator='0',
                        requirements={})

    container.subtasks.append(folderTwo)
    container.subtasks.append(subcontainer)


    graph = Graph('graphFor' + taskName, container, 'georges')

    from pprint import pprint
    pprint(graph.toRepresentation())
#    return
#    result = graph.submit("192.168.1.26", 8004)
    result = graph.submit("127.0.0.1", 8004)
#    result = graph.submit("tux095", 8004)
    print result

if __name__ == '__main__':

    for i in xrange(1):
        sendtask("testtask" + str(i))

