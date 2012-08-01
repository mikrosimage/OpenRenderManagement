import unittest
import simplejson as json
from mock import Mock

from octopus.dispatcher.webservice.rendernodes import RenderNodeController
from octopus.dispatcher.model import RenderNode

class RenderNodeControllerTestCase(unittest.TestCase):
    
    def setUp(self):
        # TODO: replace the mock framework by a nice fixture...
        self.framework = Mock()
        self.framework.application.dispatchTree.renderNodes = {}
        self.framework.application.dispatchTree.pools = {}
        self.framework.application.dispatchTree.pools['default'] = Mock()
        self.controller = RenderNodeController(self.framework, '/')
        self.renderNode = RenderNode(42, "coincoin", 64, 3.14159, "1.3.3.7", 1337)
        self.renderNodeRepresentation = {'status': 2, 'commands': [], 'name': 'coincoin', 'ip': '1.3.3.7', 'id': 42, 'isRegistered': False, 'speed': 3.14159, 'port': 1337, 'resources': {'used': 0, 'free': 64}, 'lastAliveTime': 0}
    
    def testRenderNodeRepresentation(self):
        "testing render node representation"
        r = self.controller.renderNodeRepresentation(self.renderNode)
        def explain(d0, d1):
            s = []
            for name, val in d0.items():
                if not name in d1:
                    s.append("%s missing" % name)
                elif val != d1[name]:
                    s.append("%s entry differs: %s != %s" % (repr(name), repr(val), repr(d1[name])))
            return "\n".join(s)
        self.assertEqual(r, self.renderNodeRepresentation, explain(r, self.renderNodeRepresentation))

    def testRegisterRenderNodeWithNewRenderNode(self):
        "testing render node registration with a new render node"
        request = Mock()
        request.headers = {'content-length': len(self.renderNodeRepresentation)}
        request.rfile.read.return_value = json.dumps(self.renderNodeRepresentation)
        response = self.controller.registerRenderNode(request, self.renderNode.name)
        self.assertEquals(response.status, 201)
        addRenderNode = self.framework.application.dispatchTree.pools['default'].addRenderNode
        self.assertTrue(addRenderNode.called)
        self.assertTrue(addRenderNode.call_args_list[0][0][0].name == self.renderNode.name)

    def testRegisterRenderNodeMissingContentLength(self):
        "testing render node registration: missing Content-Length header"
        request = Mock()
        request.headers = {}
        response = self.controller.registerRenderNode(request, self.renderNode.name)
        self.assertEquals(response.status, 411)
        
    def testRegisterRenderNodeNetworkErrorOnBodyRead(self):
        "testing render node registration: network error on request's body read"
        import httplib
        request = Mock()
        request.headers = {'content-length': 100}
        def raiseIncompleteRead(self):
            raise httplib.IncompleteRead("")
        request.rfile.read.side_effect = raiseIncompleteRead
        self.assertRaises(httplib.IncompleteRead, self.controller.registerRenderNode, request, self.renderNode.name)
        
    def testRegisterRenderNodeMissingRequestIdHeader(self):
        "testing render node registration: missing requestId header"
        # FIXME: implementation needed
        
    def testRegisterRenderNodeMissingRenderNodeIdHeader(self):
        "testing render node registration: missing rnId header"
        # FIXME: implementation needed


def suite():
    return unittest.makeSuite(RenderNodeControllerTestCase)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
