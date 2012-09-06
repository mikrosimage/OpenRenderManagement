from threading import Thread, Condition, Lock, Event
import httplib as http

from octopus.core.communication.requestmanager import RequestManager


class HTTPRequester(Thread):

    def __init__(self, taskList):
        super(HTTPRequester, self).__init__()
        self.taskList = taskList
        self.stopFlag = False

    def stopRequested(self):
        return self.stopFlag

    def stop(self):
        self.stopFlag = True

    def run(self):
        while True:
            self.taskList.cond.acquire()
            while not self.taskList.tasks:
                if self.stopFlag:
                    self.taskList.cond.release()
                    return
                self.taskList.cond.wait()

            # get next url to retrieve from taskList.tasks
            id, host, port, path, headers, data, method = self.taskList.tasks.pop()
            self.taskList.cond.release()

            req = RequestManager(host, port)
            try:
                data = getattr(req, method.lower())(path, data, headers)
                self.taskList.setResponse(id, data)
            except http.BadStatusLine:
                self.taskList.setResponse(id, 'BadStatusLine Error')


class HTTPRequestersSync(object):

    NOT_READY, IN_PROGRESS, READY = range(3)

    def __init__(self, threadCount=8):
        self.tasks = []
        self.cond = Condition()
        self.threads = [HTTPRequester(self) for i in xrange(threadCount)]
        self.responses = {}
        self.idCounter = 0
        self.responsesLock = Lock()
        self.pendingRequests = 0
        self.workDone = Event()
        self.prepare()
        self.start()

    def start(self):
        for thread in self.threads:
            thread.start()

    def stopAll(self):
        for thread in self.threads:
            thread.stop()
        self.cond.acquire()
        self.cond.notifyAll()
        self.cond.release()
        for thread in self.threads:
            thread.join()

    def addRequest(self, host, port, path, headers={}, data=None, method="GET"):
        self.cond.acquire()
        id = self.idCounter + 1
        self.idCounter = id
        self.tasks.append((id, host, port, path, headers, data, method))
        self.cond.notify(1)
        self.cond.release()
        self.responsesLock.acquire()
        self.pendingRequests += 1
        self.responsesLock.release()
        return id

    def setResponse(self, id, response):
        self.responsesLock.acquire()
        self.responses[id] = response
        self.pendingRequests -= 1
        self.responsesLock.release()
        self.workDone.set()

    def executeRequests(self):
        self.status = self.IN_PROGRESS
        if self.pendingRequests:
            while True:
                self.workDone.wait(5.0)
                self.workDone.clear()
                self.responsesLock.acquire()
                pendingRequests = self.pendingRequests
                self.responsesLock.release()
                if pendingRequests <= 0:
                    break
        self.status = self.READY

    def prepare(self):
        self.status = self.NOT_READY
        self.tasks = []
        self.pendingRequests = 0
        self.responses.clear()

    def getResponseByRequestId(self, id):
        if self.status == HTTPRequestersSync.READY:
            return self.responses[id]
        if self.status == HTTPRequestersSync.IN_PROGRESS:
            raise RuntimeError("The request synchronization is still in progress")
        if self.status == HTTPRequestersSync.NOT_READY:
            raise RuntimeError("You should perform an .executeRequests() call first")


if __name__ == '__main__':
    import time
    t0 = time.time()
    t = HTTPRequestersSync(8)
    requests = []
    for i in xrange(5):
        requests.append(t.addRequest("www.google.fr", 80, "/"))

    t.executeRequests()
    for id in requests:
        resp = t.getResponseByRequestId(id)
        print id, ":", "%s..." % resp[:10]

    t.stopAll()
    t0 = time.time() - t0
    print "done in %ss" % t0
