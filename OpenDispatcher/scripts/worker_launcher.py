import os
import logging   
import sys
import errno

from octopus.worker.worker import Worker
from octopus.worker.workerwebservice import WorkerWebService
from octopus.core.framework.wsappframework import WSAppFramework

BASE_LOG_DIR = "/tmp/logs/puli/"
WORKER_PORT = 8000

if __name__ == '__main__':
    
    logDir = os.path.join(BASE_LOG_DIR, "workers")
    try:
        os.makedirs(logDir, 0755)
    except OSError, e:
       if e.errno == errno.EEXIST:
           pass

    logFile = os.path.join(logDir, "worker_%d.log" % (WORKER_PORT))
    
    fileHandler = logging.FileHandler(logFile, "w", "UTF-8")
    fileHandler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(logging.Formatter("%(levelname)8s [%(name)s] %(message)s", "%x %X"))
    
    logger = logging.getLogger() 
    logger.setLevel(logging.DEBUG)

    if False:
        logger.addHandler(fileHandler)
    if True:
        logger.addHandler(consoleHandler)
    
    logging.getLogger("dispatcher").setLevel(logging.DEBUG)
    logging.getLogger("webservices").setLevel(logging.DEBUG)

    workerApplication = WSAppFramework(applicationClass = Worker, webServiceClass = WorkerWebService, port = WORKER_PORT)
    workerApplication.mainLoop()
