try:
    import simplejson as json
except ImportError:
    import json

from octopus.core.communication.http import Http400, Http405, Http411


def allowmethods(*allowed_methods):
    def allowmethods_decorator(func):
        def decorated_func(self, request, *args, **kwargs):
            if request.command not in allowed_methods:
                return Http405(allowed=allowed_methods)
            return func(self, request, *args, **kwargs)
        return decorated_func
    return allowmethods_decorator


def requireContentLength(func):
    def decorated_func(self, request, *args, **kwargs):
        if 'Content-Length' in request.headers:
            request.contentLength = int(request.headers['Content-Length'])
            return func(self, request, *args, **kwargs)
        else:
            return Http411()
    decorated_func.func_name = func.func_name
    return decorated_func


def JSONContent(func):
    @requireContentLength
    def decorated_func(self, request, *args, **kwargs):
        data = request.rfile.read(request.contentLength)
        try:
            if not isinstance(data, unicode):
                data = data.decode('utf-8', 'replace')
            request.JSON = json.loads(data)
        except ValueError:
            return Http400("Invalid JSON body.")
        return func(self, request, *args, **kwargs)
    decorated_func.func_name = func.func_name
    return decorated_func
