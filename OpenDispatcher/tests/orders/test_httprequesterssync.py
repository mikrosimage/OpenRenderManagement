from unittest import TestCase
import math

import octopus.core.communication.httprequesterssync as h


HTTPRequestersSync = h.HTTPRequestersSync


class FakeRequestManager(object):
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
    
    def request(self, method, path, data=None, headers=None):
        time.sleep(.1)
        rData = eval(path)
        return rData
    
    def get(self, path, headers={}):
        return self.request("GET", path, headers=headers)
    
    def post(self, path, data, headers={}):
        return self.request("POST", path, data, headers)
    
    def update(self, path, data, headers={}):
        return self.request("PUT", path, data, headers=headers)

PreviousRequestManager = h.RequestManager
h.RequestManager = FakeRequestManager

import random
import time

class TestPrequestersSync(TestCase):
    
    def setUp(self):
       self.numThreads = 8
       self.sync = HTTPRequestersSync(self.numThreads)

    def test_threadingPerformance(self, ExceptionType=ValueError):
        deltas = []
        
        for number in range(2,64):
            requests = []
            values = range(number)
            random.shuffle(values)
            
            expectedResponses = []
            responses = []
            
            tableNum = random.randint(1,100)
            for i in range(len(values)):
                requests.append( self.sync.addRequest("localhost", 80,str(tableNum)+"*"+str(values[i])) )
                expectedResponses.append(tableNum*values[i]) 
            startTime = time.time()
            self.sync.executeRequests()
            deltas.append([number,time.time()-startTime])
            for id in requests:
                responses.append(self.sync.getResponseByRequestId(id))
                
            self.assertEqual(responses,expectedResponses)
        
        self.sync.stopAll()
        
        
        toCompareValues = []
        for delta in deltas:
            add = 0
            if delta[0] % self.numThreads:
                add = 1
            toCompareValues.append( delta[1]/ float(math.floor(delta[0]/float(self.numThreads))+add) )
        
        average = sum(toCompareValues)/float(len(toCompareValues))
        diffs  = [math.fabs(toCompareValues[i]-average) for i in range(len(toCompareValues))]
        maxDiff = max(diffs)
        
        self.assertTrue(maxDiff<.2*average,"The execution time was to long. Threads may not be correclty used.")

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from threading import Thread



class ClientWebHandler(BaseHTTPRequestHandler):

    def do_HEAD(self):
        self.send_response(200, "OK")
        self.send_header("Content-type", "text/plain")
        self.end_headers()
    
    def do_GET(self):

        self.send_response(200, "OK")
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(self.path*10)



class ClientWebServer(HTTPServer,Thread):
    
    default_server_address = ('localhost', 8000)
    request_queue_size = 50
    
    def __init__(self, addr=default_server_address):
        HTTPServer.__init__(self, addr, ClientWebHandler)
        Thread.__init__(self)
        self.stop = 0
        
    def run(self):
    
        while not self.stop:
            self.handle_request()




class TestHTTPManager(TestCase):   
        
    def setUp(self):
        self.server = ClientWebServer()
        
    def test_connections(self):
        self.server.start()
        rm = PreviousRequestManager('localhost',8000)
        url = "Azerezrmzelkrzemlk"
        a = rm.get(url)
        self.assertEqual(a,url*10)
        self.server.stop = 1
        self.server.server_close()
        rm.get("STOP ME PLEASE")


            

     