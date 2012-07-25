import tornado
__all__ = []

import sys

# JSON library
# ------------
#
# Python 2.6+ comes with a json module.
# Older versions will use simplejson.
#
try:
    import json
except ImportError:
    import simplejson as json

# HTTP client library
#
# Python 3.0+ moved httplib into http.client
#
try:
    import http.client as httplib
except ImportError:
    import httplib

from threading import Event


class Workload(object):

    def __init__(self, job):
        self.event = Event()
        self.job = job
        self.result = None
        self.error = None

    def __call__(self):
        try:
            self.result = self.job()
        except Exception:
            self.error = sys.exc_info()

    def submit(self):
        self.event.set()

    def wait(self):
        self.event.wait()
        if isinstance(self.result, tornado.web.HTTPError):
            raise self.result
        else:
            return self.result
