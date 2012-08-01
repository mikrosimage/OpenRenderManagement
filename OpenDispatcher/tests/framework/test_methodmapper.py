import unittest
import logging

from octopus.core.framework.methodmapper import MethodMapper
from octopus.core.communication.http import HttpResponse, Http405, Http500

class Mock(object):
    
    def __init__(self, **kwargs):
        for name, value in kwargs.items():
            setattr(self, name, value)


class NullHandler(logging.Handler):
    def emit(self, record):
        pass


## A test case for the Mapping.match method
#
# This test consists of 4 parts.
#
# The first part checks that the mappings with no argument work.
# The second one checks that unnamed arguments mappings work.
# The third one checks that named arguments mappings work.
# The last one checks that a mix of named and unnamed arguments work.
#
class TestMethodMapper(unittest.TestCase):
    
    def get(self, request):
        return HttpResponse(200, 'GET')
    
    def put(self, request):
        return HttpResponse(200, 'PUT')
    
    def bad(self, request):
        raise Exception("I'm baaaad.")
    
    def setUp(self):
        logging.getLogger().addHandler(NullHandler())
    
    def testCall(self):
        mapper = MethodMapper(GET=self.get, PUT=self.put)
        
        request = Mock(command='GET')
        result = mapper(request)
        self.assertTrue(isinstance(result, HttpResponse))
        self.assertTrue(result.status==200)
        self.assertTrue(result.message=='GET')

        request = Mock(command='PUT')
        result = mapper(request)
        self.assertTrue(isinstance(result, HttpResponse))
        self.assertTrue(result.status==200)
        self.assertTrue(result.message=='PUT')
    
        request = Mock(command='POST')
        result = mapper(request)
        self.assertTrue(isinstance(result, Http405))
        self.assertTrue(result.status==405)

    def testInternalErrorHandler(self):
        mapper = MethodMapper(GET=self.bad)

        request = Mock(command='GET', path="/go/to/hell")
        result = mapper(request)
        self.assertTrue(isinstance(result, Http500))
        self.assertTrue(result.status==500)

        