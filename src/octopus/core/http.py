import httplib
import socket

class Request(object):
    '''A class that encapsulates a HTTP request and its execution.

    Example:
    >>> import httplib
    >>> conn = httplib.HTTPConnection("localhost", 8004)
    >>> def onResponse(request, response):
    ...   print "%s %s ==> %d %s" % (request.method, request.path, response.status, response.reason)
    ...   print response.read()
    >>> def onError(request, error):
    ...   print "%s %s ==> %s" % (request.method, request.path, error)
    >>> r = Request("GET", "/nodes/0", {"Accept": "application/json"}, "")
    >>> r.call(conn, onResponse, onError)
    GET /nodes/0 ==> 200 OK
    {}
    '''
    
    def __init__(self, method, path, headers={}, body=''):
        self.method = method
        self.path = path
        self.headers = headers
        self.body = body
        
    def call(self, conn, onResponse, onError):
        try:
            conn.request(self.method, self.path, self.body, self.headers)
            response = conn.getresponse()
        except (EnvironmentError, httplib.error, socket.error), e:
            onError(self, e)
        except:
            onError(self, None)
        else:
            onResponse(self, response)
            if response.length:
                response.read()

