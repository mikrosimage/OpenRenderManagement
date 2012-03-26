import os
import logging   
 

from octopus.worker.worker import Worker
from octopus.worker.workerwebservice import WorkerWebService
from octopus.core.framework.wsappframework import WSAppFramework


WORKER_PORT = 8002

if __name__ == '__main__':
    
    fileHandler = logging.FileHandler(os.path.dirname(__file__)+"/../logs/workers/worker_%d.log" % WORKER_PORT, "w", "UTF-8")
    fileHandler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    
    logger = logging.getLogger() 
    logger.addHandler(fileHandler)
    logger.setLevel(logging.DEBUG)

    workerApplication = WSAppFramework(applicationClass = Worker, webServiceClass = WorkerWebService, port = WORKER_PORT)
    workerApplication.mainLoop()