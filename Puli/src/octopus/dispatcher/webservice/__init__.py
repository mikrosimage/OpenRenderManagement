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


from octopus.core import singletonconfig, singletonstats
from octopus.core.framework import BaseResource

class DispatcherBaseResource(BaseResource):
    """
    Simply override prepare to have a specific handler for the dispatcher (stats are not allowed for the worker)
    """
    
    def prepare( self ):
        """
        For each request, update stats if needed
        """
        if singletonconfig.get('CORE','GET_STATS'):
            singletonstats.theStats.cycleCounts['incoming_requests'] += 1

            if self.request.method == 'GET':
                    singletonstats.theStats.cycleCounts['incoming_get'] += 1
            elif self.request.method == 'POST':
                    singletonstats.theStats.cycleCounts['incoming_post'] += 1
            elif self.request.method == 'PUT':
                    singletonstats.theStats.cycleCounts['incoming_put'] += 1
            elif self.request.method == 'DELETE':
                    singletonstats.theStats.cycleCounts['incoming_delete'] += 1

from .webservicedispatcher import WebServiceDispatcher as WebService
