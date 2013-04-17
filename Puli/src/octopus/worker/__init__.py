from octopus.core.framework import WSAppFramework
from octopus.worker import settings
from octopus.worker.worker2 import Worker2
from octopus.worker.workerwebservice import WorkerWebService


def make_worker():
    '''Returns an instance of the worker application.'''
    return WSAppFramework(Worker2, WorkerWebService, settings.PORT)
