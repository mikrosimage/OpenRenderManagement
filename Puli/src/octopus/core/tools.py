import tornado
import sys
import logging
import time
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


def elapsedTimeToString(timer):
    '''   
    :return: String representing the time elapsed since the timer value given in format H:M:S
    '''
    LOGGER = logging.getLogger('dispatcher')

    try:
      return time.strftime('%H:%M:%S', time.gmtime(time.time()-timer))
    except Exception,e:
      LOGGER.error("A problem occured when calculating elapsed time (%r)"%e)
      return ""