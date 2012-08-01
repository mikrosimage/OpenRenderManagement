from octopus.core import framework
from octopus.core.tools import Workload


def queue(func):
    def queued_func(self, *args, **kwargs):
        return self.queueAndWait(func, self, *args, **kwargs)
    return queued_func


class Controller(framework.Controller):

    def queueAndWait(self, func, *args):
        workload = Workload(lambda: func(*args))
        self.framework.application.queueWorkload(workload)
        return workload.wait()

    @property
    def dispatcher(self):
        return self.framework.application

from .webservicedispatcher import WebServiceDispatcher as WebService
