#    tree = DispatchTree()
#    task = Task(None, "tsk task", 42, 36, "coin", [], "", [])
#    print tree.tasks
#    print "task in tree.tasks:", (task.id, task) in tree.tasks.items()
#    print "task.id =", task.id
#    pool = Pool(42, "default")
#    print "pool in tree.pools:", (pool.name, pool) in tree.pools.items()
#    print "pool.id =", pool.id
#    pool = Pool(None, "default2")
#    print "pool in tree.pools:", (pool.name, pool) in tree.pools.items()
#    print "pool.id =", pool.id

import unittest
import simplejson as json
from mock import Mock, Sentinel
import weakref

from octopus.dispatcher.model import DispatchTree, Task, Pool, FolderNode
from octopus.dispatcher.strategies import FifoStrategy


class DispatchTreeTestCase(unittest.TestCase):
    
    def setUp(self):
        ""
        self.tree = DispatchTree()
        self.user = "bud"
    
    def tearDown(self):
        if getattr(self, 'tree', None) is not None:
            self.tree.destroy()
    
    def testCheckRootNode(self):
        """testing dispatch tree default root node"""
        root = self.tree.root
        self.assertTrue(isinstance(root, FolderNode))
        self.assertEquals(root.name, "root")
        self.assertEquals(root.user, "root")
        self.assertTrue(root.parent is None)
        self.assertEquals(root.priority, 1)
        self.assertEquals(root.dispatchKey, 1)
        self.assertEquals(root.maxRN, 0)
        self.assertTrue(isinstance(root.strategy, FifoStrategy))
    
    def testTaskRegistration(self):
        """testing dispatch tree task auto-registration in the tree"""
        # FIXME: implement me!
        # FIXME: watch out for multiple tree creation?
        t = FolderNode(None, "testfoldernode", None, self.user, 0, 0, 0, FifoStrategy())
        tref = weakref.ref(t)
        self.assertTrue(t in self.tree.nodes.values())
        t.parent = None
        self.tree.destroy()
        del self.tree
        del t
        self.assertTrue(tref() is None)
    
    def testFindNodeByPath(self):
        """testing dispatch tree findNodeByPath"""
        priority = 0
        dispatchKey = 0
        maxRN = 0
        strategy = FifoStrategy()
        graphs = FolderNode(None, "graphs", self.tree.root, self.user, priority, dispatchKey, maxRN, strategy)
        graph1 = FolderNode(None, "graph1", graphs, self.user, priority, dispatchKey, maxRN, strategy)
        subgraph1 = FolderNode(None, "subgraph1", graph1, self.user, priority, dispatchKey, maxRN, strategy)

        self.assertEquals(self.tree.findNodeByPath("/"), self.tree.root)
        self.assertEquals(self.tree.findNodeByPath("/graphs"), graphs)
        self.assertEquals(self.tree.findNodeByPath("/graphs/graph1"), graph1)
        self.assertEquals(self.tree.findNodeByPath("/graphs/graph1/subgraph1"), subgraph1)
        self.assertEquals(self.tree.findNodeByPath("/"), self.tree.root)
        self.assertEquals(self.tree.findNodeByPath("/graphs/"), graphs)
        self.assertEquals(self.tree.findNodeByPath("/graphs/graph1/"), graph1)
        self.assertEquals(self.tree.findNodeByPath("/graphs/graph1/subgraph1/"), subgraph1)
        self.assertEquals(self.tree.findNodeByPath(""), self.tree.root)
        self.assertEquals(self.tree.findNodeByPath("graphs/"), graphs)
        self.assertEquals(self.tree.findNodeByPath("graphs/graph1/"), graph1)
        self.assertEquals(self.tree.findNodeByPath("graphs/graph1/subgraph1/"), subgraph1)

    def testRegisterNewGraph(self):
        """testing dispatch tree registerNewGraph"""
        rule1 = Mock()
        self.tree.rules.append(rule1)
        graph1 = Mock()
        self.tree.registerNewGraph(graph1)
        rule1.assert_called_with(graph1)
        rule2 = Mock()
        self.tree.rules.append(rule2)
        self.tree.registerNewGraph(graph1)
        rule1.assert_called_with(graph1)
        rule2.assert_called_with(graph1)

def suite():
    return unittest.makeSuite(DispatchTreeTestCase)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
