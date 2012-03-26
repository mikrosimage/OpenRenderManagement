import unittest
import simplejson as json
from mock import Mock

from octopus.dispatcher.webservice.nodes import *
from dispatcher.webservices.controllers import ControllerTestCase

class NodeRepresentationExtractDataOnFolderNodeTestCase(unittest.TestCase):
    
    def setUp(self):
        from octopus.dispatcher.model import FolderNode
        from octopus.dispatcher.strategies import FairStrategy
        self.user = "bud"
        folderParentNode = FolderNode(36, "a test node", None, self.user, 1, 0, -1, FairStrategy())
        self.folderNode = FolderNode(37, "a test node", folderParentNode, self.user, 1, 0, -1, FairStrategy())
        self.odict = NodeRepresentation.extractData(self.folderNode)
    
    def tearDown(self):
        self.folderNode.parent = None
        del self.folderNode
        del self.odict
    
    def testID(self):
        """Checking ID in extracted data"""
        self.assertEquals(self.odict[NodeRepresentation.ID], self.folderNode.id)
    
    def testParentID(self):
        """Checking parent ID in extracted data"""
        self.assertEqual(self.odict[NodeRepresentation.PARENT_ID], self.folderNode.parent.id)
    
    def testUser(self):
        """Checking user name in extracted  data"""
        self.assertEqual(self.odict[NodeRepresentation.USER_NAME], self.user)
    
    def testName(self):
        """Checking name in extracted data"""
        self.assertEqual(self.odict[NodeRepresentation.NAME], self.folderNode.name)
    
    def testPriority(self):
        """Checking priority in extracted data"""
        self.assertEqual(self.odict[NodeRepresentation.PRIORITY], self.folderNode.priority)
    
    def testDispatchKey(self):
        """Checking dispatch key in extracted data"""
        self.assertEqual(self.odict[NodeRepresentation.DISPATCH_KEY], self.folderNode.dispatchKey)
    
    def testMaxRN(self):
        """Checking max RN in extracted data"""
        self.assertEqual(self.odict[NodeRepresentation.MAX_RN], self.folderNode.maxRN)
    
    def testStatus(self):
        """Checking status in extracted data"""
        self.assertEqual(self.odict[NodeRepresentation.STATUS], self.folderNode.status)
    
    def testNodeType(self):
        """Checking node type in extracted data"""
        self.assertEqual(self.odict[NodeRepresentation.NODE_TYPE], NodeRepresentation.FOLDER_NODE)
    
    def testChildList(self):
        """Checking child list in extracted data"""
        self.assertEqual(self.odict[NodeRepresentation.CHILD_LIST], [child.id for child in self.folderNode.children])
    
    def testStrategy(self):
        """Checking strategy in extracted data"""
        self.assertEqual(self.odict[NodeRepresentation.STRATEGY], NodeRepresentation.getStrategyName(self.folderNode.strategy))


class NodeNotFoundExceptionTestCase(ControllerTestCase):
    
    def testBasicConstruction(self):
        """Checking basic call NodeNotFoundError(node)"""
        node = Mock()
        e = NodeNotFoundError(node)
        self.assertTrue(e.node is node)
    
    def testCustomConstruction(self):
        """Checking call NodeNotFoundError(node, "coin", plop=42)"""
        node = Mock()
        e = NodeNotFoundError(node, "coin", plop=42)
        self.assertTrue(e.node is node)
        self.assertEqual(e.args, ("coin",))
        self.assertEqual(e.plop, 42)

class SetPriorityTestCase(ControllerTestCase):
    
    def setUp(self):
        self.addSimpleDispatchTree(fill=True, application=True, framework=True)
        self.controller = NodeController(self.framework, self.dispatchTree.root)
        self.node = self.dispatchTree.nodes[1]
        self.nodeId = 1
        self.request = Mock(spec=['read'])
        self.request.rfile = Mock()
        body = '{"priority": 42}'
        self.request.rfile.read.return_value = body
        self.request.headers = {"Content-Length": len(body)}
           
    def tearDown(self):
        self.dispatchTree.destroy()

    def testReturnValue(self):
        '''NodeController.setNodePriority: Checking return value'''
        from octopus.core.communication import HttpResponse
        value = self.controller.setNodePriority(self.request, self.nodeId)
        self.assertTrue(isinstance(value, HttpResponse))
    
    def testAddOrderCall(self):
        '''NodeController.setNodePriority: Checking that the correct order was pushed'''
        value = self.controller.setNodePriority(self.request, self.nodeId)
        self.assertEqual(self.framework.addOrder.call_args[0], (self.controller._setNodePriority, self.nodeId, 42))

class SetDispatchKeyTestCase(ControllerTestCase):
    
    def setUp(self):
        self.addSimpleDispatchTree(fill=True, application=True, framework=True)
        self.controller = NodeController(self.framework, self.dispatchTree.root)
        self.node = self.dispatchTree.nodes[1]
        self.nodeId = 1
        self.request = Mock(spec=['read'])
        self.request.rfile = Mock()
        body = '{"dispatchKey": 42}'
        self.request.rfile.read.return_value = body
        self.request.headers = {"Content-Length": len(body)}
           
    def tearDown(self):
        self.dispatchTree.destroy()

    def testReturnValue(self):
        '''NodeController.setNodeDispatchKey: Checking return value'''
        from octopus.core.communication import HttpResponse
        value = self.controller.setNodeDispatchKey(self.request, self.nodeId)
        self.assertTrue(isinstance(value, HttpResponse))
    
    def testAddOrderCall(self):
        '''NodeController.setNodeDispatchKey: Checking that the correct order was pushed'''
        value = self.controller.setNodeDispatchKey(self.request, self.nodeId)
        self.assertEqual(self.framework.addOrder.call_args[0], (self.controller._setNodeDispatchKey, self.nodeId, 42))

class SetMaxRNTestCase(ControllerTestCase):
    
    def setUp(self):
        self.addSimpleDispatchTree(fill=True, application=True, framework=True)
        self.controller = NodeController(self.framework, self.dispatchTree.root)
        self.node = self.dispatchTree.nodes[1]
        self.nodeId = 1
        self.request = Mock(spec=['read'])
        self.request.rfile = Mock()
        body = '{"maxRN": 42}'
        self.request.rfile.read.return_value = body
        self.request.headers = {"Content-Length": len(body)}
           
    def tearDown(self):
        self.dispatchTree.destroy()

    def testReturnValue(self):
        '''NodeController.setNodeMaxRN: Checking return value'''
        from octopus.core.communication import HttpResponse
        value = self.controller.setNodeMaxRN(self.request, self.nodeId)
        self.assertTrue(isinstance(value, HttpResponse))
    
    def testAddOrderCall(self):
        '''NodeController.setNodeMaxRN: Checking that the correct order was pushed'''
        value = self.controller.setNodeMaxRN(self.request, self.nodeId)
        self.assertEqual(self.framework.addOrder.call_args[0], (self.controller._setNodeMaxRN, self.nodeId, 42))

class SetStrategyTestCase(ControllerTestCase):
    
    def setUp(self):
        self.addSimpleDispatchTree(fill=True, application=True, framework=True)
        self.controller = NodeController(self.framework, self.dispatchTree.root)
        self.node = self.dispatchTree.nodes[1]
        self.nodeId = 1
        self.request = Mock(spec=['read'])
        self.request.rfile = Mock()
        body = '{"strategy": "octopus.dispatcher.strategies.FairStrategy"}'
        self.request.rfile.read.return_value = body
        self.request.headers = {"Content-Length": len(body)}
           
    def tearDown(self):
        self.dispatchTree.destroy()

    def testReturnValue(self):
        '''NodeController.setNodeStrategy: Checking return value'''
        from octopus.core.communication import HttpResponse
        value = self.controller.setNodeStrategy(self.request, self.nodeId)
        self.assertTrue(isinstance(value, HttpResponse))

    def testAddOrderCall(self):
        '''NodeController.setNodeStrategy: Checking that the correct order was pushed'''
        value = self.controller.setNodeStrategy(self.request, self.nodeId)
        self.assertEqual(self.framework.addOrder.call_args[0], (self.controller._setNodeStrategy, self.nodeId, 'octopus.dispatcher.strategies.FairStrategy'))

def suite():
    testCases = [
        NodeRepresentationExtractDataOnFolderNodeTestCase,
        NodeNotFoundExceptionTestCase,
        SetPriorityTestCase,
        SetDispatchKeyTestCase,
        SetMaxRNTestCase,
        SetStrategyTestCase,
    ]
    suites = [unittest.makeSuite(testCase) for testCase in testCases]
    return unittest.TestSuite(suites)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
