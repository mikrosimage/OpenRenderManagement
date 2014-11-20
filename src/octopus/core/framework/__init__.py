import tornado
import logging
import httplib
try:
    import simplejson as json
except ImportError:
    import json

from octopus.core.framework.wsappframework import WSAppFramework, MainLoopApplication
from octopus.core.framework.webservice import MappingSet

from octopus.core.communication.http import Http400
from octopus.core.tools import Workload


__all__ = ['WSAppFramework', 'MainLoopApplication']
__all__ += ['Controller', 'ControllerError', 'ResourceNotFoundErro', 'BaseResource']

logger = logging.getLogger('main.dispatcher.webservice')


def queue(func):
    def queued_func(self, *args, **kwargs):
        return self.queueAndWait(func, self, *args, **kwargs)
    return queued_func


class ControllerError(Exception):
    """
    Raised by a controller to report a problem. Subclass at will.
    """


class ResourceNotFoundError(ControllerError):
    """
    Raised by a controller to report access to a missing resource.
    """
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args)
        for name, value in kwargs.items():
            setattr(self, name, value)


class Controller(object):

    def __init__(self, framework, root):
        self.framework = framework
        self.root = root
        self.mappings = MappingSet()

    def __call__(self, request, path, *args):
        return self.mappings.match(request, path, *args)

    def getDispatchTree(self):
        return self.framework.application.dispatchTree

    def map(self, pathPattern, methodDict):
        self.mappings.add((pathPattern, methodDict))


class BaseResource(tornado.web.RequestHandler):
    def initialize(self, framework):
        self.framework = framework

    def getDispatchTree(self):
        return self.framework.application.dispatchTree

    def get_error_html(self, status_code, exception=None, **kwargs):
        message = httplib.responses[status_code]
        if exception is not None and isinstance(exception, tornado.web.HTTPError):
            message = exception.log_message
        return "%(code)d: %(message)s" % {
            "code": status_code,
            "message": message,
        }

    @property
    def dispatcher(self):
        return self.framework.application

    def getBodyAsJSON(self):
        try:
            if self.request.body == "" or self.request.body is None:
                return ""
            return json.loads(self.request.body)
        except:
            raise Http400("The HTTP body is not a valid JSON object")

    def getServerAddress(self):
        server_address = self.request.host.split(':')
        if len(server_address) == 2:
            return server_address[0], server_address[1]
        return server_address[0], ""

    def queueAndWait(self, func, *args):
        workload = Workload(lambda: func(*args))
        self.framework.application.queueWorkload(workload)
        return workload.wait()

    def writeCallback(self, chunk):
        data = self.request.arguments
        if 'callback' in data:
            chunk = ('%s(%s);' % (data['callback'][0], chunk))
        self.write(chunk)
