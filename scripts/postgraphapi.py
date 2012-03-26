# encoding: UTF-8
from octopus.client import *
import urlparse

def sendtask():

    container = Task(name = ('task /container/ éàè'),
                     jobtype = '',
                     arguments = {},
                     dependencies = [], 
                     subtasks = [],
                     maxrn = 1,
                     validator = '')
        
    task = Task(name ='test task',
                jobtype = 'PrintInFile',
                arguments = { 'text': 'Lorem ipsum dolor sit amet, consectetur ' +
                                      'adipisicing elit, sed do eiusmod tempor ' +
                                      'incididunt ut labore et dolore magna ' +
                                      'aliqua. Ut enim ad minim veniam, quis ' +
                                      'nostrud exercitation ullamco laboris nisi ' +
                                      'ut aliquip ex ea commodo consequat. Duis aute ' +
                                      'irure dolor in reprehenderit in voluptate velit ' +
                                      'esse cillum dolore eu fugiat nulla pariatur. ' +
                                      'Excepteur sint occaecat cupidatat non proident, ' +
                                      'sunt in culpa qui officia deserunt mollit anim ' +
                                      'id est laborum.' },
                dependencies = [], 
                subtasks = [],
                maxrn = 1,
                validator = '')
    
    task.decompose()

    container.subtasks.append(task)
    
    subcontainer = Task(name ='sub task container 2',
                        jobtype = '',
                        arguments = {},
                        dependencies = [], 
                        subtasks = [task],
                        maxrn = 1,
                        validator = '')
    
    container.subtasks.append(subcontainer)
    
    graph = Graph('test graph', [])
    graph.tasks.append(container)
    
    result = graph.submit("localhost", 8004)

if __name__ == '__main__':
    
    for i in xrange(3):
        sendtask()
    
