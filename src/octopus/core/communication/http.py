#!/usr/bin/env python
####################################################################################################
# @file http.py
# @package 
# @author bud
# @date 2009/01/12
# @version 0.1
#
# @mainpage
# 
####################################################################################################

from tornado.web import HTTPError
import StringIO

class HttpResponse(dict):
    
    def __init__(self, status=200, message='OK', content='', contenttype='application/json'):
        super(HttpResponse, self).__init__()
        self.content = StringIO.StringIO(content)
        self.status = status
        self.message = message
        self['Content-Type'] = contenttype

    def write(self, *args, **kwargs):
        return self.content.write(*args, **kwargs)
    
    def send(self, handler):
        content = self.content.getvalue()
        if content and 'Content-Length' not in self:
            self['Content-Length'] = len(content)
        handler.send_response(self.status, self.message)
        for header, value in self.iteritems():
            handler.send_header(header, value)
        handler.end_headers()
        handler.wfile.write(content)


class JSONResponse(HttpResponse):
    
    def __init__(self, status, message, data):
        from octopus.core.tools import json
        content = json.dumps(data)
        HttpResponse.__init__(self, status, message, content=content)
        #json.dump(data, self.content)

## A basic HttpResponse for error 400 (Bad request)
#
class Http400(HTTPError):
    """A basic HttpResponse for error 400 (Bad request)"""
    
    def __init__(self, msg="Bad request", **kwargs):
        super(Http400, self).__init__(400, msg)


## A basic HttpResponse for error 405 (Method not allowed)
#
class Http405(HTTPError):
    """A basic HttpResponse for error 405 (Method not allowed)"""
    
    def __init__(self, allowed, **kwargs):
        super(Http405, self).__init__(405, "Method not allowed")
        self['Allow'] = ', '.join(allowed)


## A basic HttpResponse for error 403 (Action forbidden)
#
class Http403(HTTPError):
    """A basic HttpResponse for error 403 (Action forbidden)"""

    def __init__(self, message="Action forbidden", content='', contenttype='text/plain'):
        super(Http403, self).__init__(403, message)


## A basic HttpResponse for error 404 (Resource not found)
#
class Http404(HTTPError):
    """A basic HttpResponse for error 404 (Resource not found)"""

    def __init__(self, message="Resource not found", content='', contenttype='text/plain'):
        super(Http404, self).__init__(404, message,)


## A basic HttpResponse for error 409 (Conflict)
#
class HttpConflict(HTTPError):
    """A basic HttpResponse for error 409 (Conflict)"""

    def __init__(self, message="Conflict"):
        super(HttpConflict, self).__init__(409, message)


## A basic HttpResponse for error 411 (Length Required)
#
class Http411(HTTPError):
    """A basic HttpResponse for error 411 (Length Required)"""

    def __init__(self):
        super(Http411, self).__init__(411, "Length Required")


## A basic HttpResponse for error 500 (Internal server error)
#
class Http500(HTTPError):
    """A basic HttpResponse for error 500 (Internal server error)"""
    
    def __init__(self):
        super(Http500, self).__init__(500, "Internal server error")

