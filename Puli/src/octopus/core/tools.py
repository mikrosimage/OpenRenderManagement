import tornado
import sys
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
