from octopus.core.framework import WSAppFramework

from .dispatcher import Dispatcher
from .webservice.webservicedispatcher import WebServiceDispatcher

def make_dispatcher():
    return WSAppFramework(applicationClass=Dispatcher, webServiceClass=WebServiceDispatcher, port=8004)

__all__ = ['make_dispatcher', 'settings']
