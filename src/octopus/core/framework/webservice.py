#!/usr/bin/env python
####################################################################################################
# @file webservice.py
# @package 
# @author acs, jbs
# @date 2008/12/01
# @version 0.1
#
# @mainpage
# 
####################################################################################################

from threading import Thread
import logging
import cgi

from octopus.core.communication.http import Http404, Http500
from methodmapper import MethodMapper

logger = logging.getLogger("framework.webservice")

class ResponseDeferred(BaseException):
    "Raised when the response to a HTTP request will be sent by another component."

## This class defines the thread that holds the instance of the webservice.
#
class WebService(Thread):
    
    def __init__(self, framework, port):
        '''Constructs a new Thread for the web service.
        
        @param framework the  application framework instance
        @param port the port the service is  to run onto
        '''
        self.framework = framework
        self.port = port
        self.httpd = None
        Thread.__init__(self)
        self.setDaemon(True)
        # append the correct Mapping
        self.mappings = MappingSet()
        self.mappings.add(Mapping('^/orders/(\d+)-(\d+)/$', self.framework.addOrder))
    
    
    ## The run method.
    #
    def run(self):
        HOST_NAME = ''
        PORT_NUMBER = self.port
        logger.info("Starting webservice on %s:%s", HOST_NAME, PORT_NUMBER)
        self.httpd = ClientWebServer(self, (HOST_NAME, PORT_NUMBER))
        self.httpd.serve_forever()
        self.httpd.server_close()
        logger.info("Stopped webservice on %s:%s", HOST_NAME, PORT_NUMBER)


    def stop(self):
        HOST_NAME = ''
        PORT_NUMBER = self.port
        if self.httpd:
            logger.info("Stopping webservice on %s:%s", HOST_NAME, PORT_NUMBER)
            self.httpd.stop()
        
    
    def handle_request(self, request):
        return self.mappings.match(request)

    
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import re



## This class puts in correlation a regular expression and a callback method.
#
class Mapping(object):
    
    def __init__(self, regexp, callback):
        self.regexp = re.compile(regexp)
        if callable(callback):
            self.callback = callback
        elif isinstance(callback, dict):
            self.callback = MethodMapper(**callback)
        else:
            raise RuntimeError("callback must be a callable or a dict mapping http methods to callables")


    ## Calls the callback method coresponding to the provided path, if it exists.
    # @param path the path to match against an existing regexp  
    #
    def match(self, request, path):        
        retval = re.match(self.regexp, path)
        if retval:
            args = retval.groups()
            kwargs = retval.groupdict()
            if kwargs:
                return self.callback(request, **kwargs)
            else:
                return self.callback(request, *[arg if arg is not None else "" for arg in args])
        else:
            return None


## This class defines a set of mappings.
#
class MappingSet(object):
    
    def __init__(self, *mappings):
        self.mappings = []
        for mapping, callback in mappings:
            self.add(Mapping(mapping, callback))
    
    
    ## Adds the provided mapping to the set.
    # @param mapping the mapping object to add
    #
    def add(self, mapping, *moremappings):
        # FIXME: this needs some more bullet proofing
        if isinstance(mapping, Mapping):
            self.mappings.append(mapping)
        elif isinstance(mapping, tuple):
            path, callback = mapping
            self.mappings.append(Mapping(path, callback))
        for onemoremapping in moremappings:
            self.add(onemoremapping)
    
    ## 
    #
    def match(self, request, path=None):
        if path is None:
            path = request.path
        for mapping in self.mappings:
            response = mapping.match(request, path)
            if response:
                return response
        else:
            return Http404()



## This class defines the request handler.
#
class ClientWebHandler(BaseHTTPRequestHandler):
    #protocol_version = "HTTP/1.1"
    request_queue_size = 32
    
    def doRequest(self):
        logger = logging.getLogger('webservice')
        try:
            self.path, query = self.path.split('?', 1)
        except ValueError:
            self.GET = {}
        else:
            self.GET = cgi.parse_qs(query)
        
        try:
            response = self.server.webservice.handle_request(self)
        except ResponseDeferred:
            return
        except Exception, e:
            logger.exception('Request raised an unexpected exception. Please send a bug report.')
            response = Http500()
        
        if response:
            response.send(self)


    def do_GET(self):
        self.doRequest()


    def do_POST(self):
        self.doRequest()


    def do_PUT(self):
        self.doRequest()

        
    def do_DELETE(self):
        self.doRequest()


    def log_error(self, format, *args):
        logger.error(format, *args)
    

    def log_message(self, format, *args):
        logger.info(format, *args)


import SocketServer

class WebserviceStopping(BaseException):
    pass

## This class represents the http server.
#
class ClientWebServer(SocketServer.ThreadingMixIn, HTTPServer):
    
    default_server_address = ('', 8000)
    request_queue_size = 50
    allow_reuse_address = True
    daemon_threads = True
    
    def __init__(self, webservice, addr=default_server_address):
        HTTPServer.__init__(self, addr, ClientWebHandler)
        self.webservice = webservice
        self.done = False


    def serve_forever(self):
        while not self.done:
            try:
                self.handle_request()
            except WebserviceStopping:
                break

    def get_request(self):
        import socket
        timeout = self.socket.gettimeout()
        #self.socket.settimeout(0.1)
        while True:
            if self.done:
                raise WebserviceStopping
            try:
                return self.socket.accept()
                self.socket.settimeout(timeout)
            except socket.error:
                self.socket.settimeout(timeout)
            finally:
                self.socket.settimeout(timeout)

    def stop(self):
        """Stops the service."""
        self.done = True
