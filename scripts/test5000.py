# encoding: UTF-8
from octopus.client import *
from octopus.core.enums.node import *
import urlparse
import random

users = ['georges', 'bud', 'acs', 'jbs']

def sendtask(taskName):

    container = Task(name = taskName,
                     jobtype = '',
                     arguments = {},
                     dependencies = {}, 
                     subtasks = [],
                     maxrn = 1,
                     validator = '0')
        
    task = Task(name ='testtask',
                jobtype = 'octopus.core.jobtypes.printinfile.PrintInFile',
                arguments = { 'text': 'plop' },
                dependencies = {}, 
                subtasks = [],
                maxrn = 1,
                validator = '0')
    
    task2 = Task(name ='testtask',
                jobtype = 'octopus.core.jobtypes.printinfile.PrintInFile',
                arguments = { 'text': 'plop' },
                dependencies = {}, 
                subtasks = [],
                maxrn = 1,
                validator = '0')
    
    task.decompose()

    container.subtasks.append(task)
    
    subcontainer = Task(name ='subtask',
                        jobtype = '',
                        arguments = {},
                        dependencies = {task: [NODE_DONE]}, 
                        subtasks = [task2],
                        maxrn = 1,
                        validator = '0')
    
    container.subtasks.append(subcontainer)
    
    user = random.choice(users)
    graph = Graph('graphFor' + taskName, container, user)
    
    result = graph.submit("127.0.0.1", 8004)

if __name__ == '__main__':
    
    for i in xrange(100):
        sendtask("testtask" + str(i))

