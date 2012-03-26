# encoding: UTF-8
from octopus.client import *
from octopus.core.enums.node import *
import urlparse
import random

def sendtask(taskName):

    container = Task(name = taskName,
                     jobtype = '',
                     arguments = {},
                     dependencies = {}, 
                     subtasks = [],
                     maxrn = 1,
                     validator = '0')
   
    for i in range(10):
        min = random.randint(0,4)
        max = random.randint(min,5)
        if max == 5: max = 0
        ram = 1000*random.randint(1,4)
        occ = str(random.randint(10,100))
        tmpTask = Task(name ='Task'+str(i).zfill(2),
                    jobtype = 'octopus.core.jobtypes.printinfile.PrintInFile',
                    arguments = { 'text': 'plopZZZ','occ':occ },
                    dependencies = {}, 
                    subtasks = [],
                    maxrn = 1,
                    validator = '0',
                    requirements = {},
                    minNbCores = min,
                    maxNbCores = max,
                    ramUse = ram
                    )

        container.subtasks.append(tmpTask)


    
    graph = Graph('graphFor' + taskName, container, 'georges')
    
#    result = graph.submit("192.168.1.26", 8004)
#    from pprint import pprint
#    pprint(graph.toRepresentation())
#    return
    result = graph.submit("127.0.0.1", 8004)
    print result

if __name__ == '__main__':
    
    for i in xrange(1):
        sendtask("testtask" + str(i))
    
