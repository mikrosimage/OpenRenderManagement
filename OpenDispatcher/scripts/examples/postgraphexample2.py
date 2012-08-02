import simplejson as json
import httplib

from api  import *

if __name__ == '__main__':
    
    container = Task(name ='task container',
                 jobtype = '',
                 arguments = {},
                 dependencies = [], 
                 subtasks = [],
                 maxrn = 1,
                 validator = '')
    
    
    task = Task(
            name= 'test task',
            jobtype= 'octopus.core.jobtypes.mayademo.MayaDemo',
            arguments= { 'nbChunks':'3','r': 'mr','s':'1','e':'100','rd':'D:\maya\images','im':'coucou','cam':'persp','scene': 'D:\maya\scene.ma' },
            dependencies= [], 
            subtasks= [],
            validator= 'VAL_TRUE',
            maxrn= 1,
            requirements = { "cores": 4, "ram": 2000, "freq": 2.0, "softs": ["maya2009","max2009"] }           
         
    )
    
        
    task.decompose()
    
    print task.subtasks
    task.subtasks[0].decompose()
    
    container.subtasks.append(task)
    
    graph = Graph('test graph', container)
    
    result = graph.submit("localhost", 8004)
    

