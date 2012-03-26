import sys
import os

from octopus.dispatcher.dispatcher import Dispatcher
from octopus.core.framework.wsappframework import WSAppFramework
from octopus.dispatcher.webservice.webservicedispatcher import WebServiceDispatcher

import logging

if __name__ == '__main__':

    fileHandler = logging.FileHandler(os.path.dirname(__file__)+"/../logs/dispatcher/dispatcher.log", "w", "UTF-8")
    fileHandler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logging.Formatter("%(name)10s %(levelname)6s %(message)s"))
    consoleHandler.setLevel(logging.DEBUG)
    #consoleHandler.setLevel(logging.CRITICAL)
    
    logger = logging.getLogger() 
    logger.addHandler(fileHandler)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(consoleHandler)

    logging.getLogger('dispatcher').setLevel(logging.CRITICAL)
    logging.getLogger('webservice').setLevel(logging.DEBUG)

    dispatcherApplication = WSAppFramework(applicationClass = Dispatcher, webServiceClass = WebServiceDispatcher, port = 8004)
    dispatcherApplication.mainLoop()



