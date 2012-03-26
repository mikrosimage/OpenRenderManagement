import unittest
import simplejson as json
from mock import Mock

from octopus.core.communication import HttpResponse
from octopus.dispatcher.webservice.tasks import TaskController
from octopus.dispatcher.model import Task, TaskNode
from dispatcher.webservices.controllers import ControllerTestCase

class BaseTaskControllerTestCase(ControllerTestCase):

    NAME = "TaskController"

    def setUp(self):
        self.addSimpleDispatchTree(True, application=True, framework=True)
        self.controller = TaskController(self.framework, "/tasks/")
    
    def tearDown(self):
        self.dispatchTree.destroy()

    def testReturnValueType(self):
        '''Checking return value type'''
        self.assertTrue(isinstance(self.response, HttpResponse))

    def testReturnValueStatus(self):
        '''Checking return value status'''
        self.assertEqual(self.response.status, 200)

    def testReturnValueContentType(self):
        '''Checking return value content type'''
        self.assertEqual(self.response['Content-Type'], 'application/json')


class ListTaskTestCase(BaseTaskControllerTestCase):
    
    def setUp(self):
        super(ListTaskTestCase, self).setUp()
        self.request = Mock()
        self.request.rfile.read.return_value = ""
        self.request.headers = {}
        self.response = self.controller.listTasks(self.request)


class GetTaskTestCase(BaseTaskControllerTestCase):

    def setUp(self):
        super(GetTaskTestCase, self).setUp()
        self.request = Mock()
        self.request.rfile.read.return_value = ""
        self.request.headers = {}
        self.response = self.controller.getTask(self.request, "1")


def suite():
    testCases = [
        ListTaskTestCase,
        GetTaskTestCase,
    ]
    suites = [unittest.makeSuite(testCase) for testCase in testCases]
    return unittest.TestSuite(suites)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
