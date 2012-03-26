import unittest

from mock import Mock, sentinel
from octopus.dispatcher.model import *
from octopus.dispatcher.strategies import loadStrategyClass
from octopus.core.framework.ticket import Ticket

FifoStrategy = loadStrategyClass("octopus.dispatcher.strategies.FifoStrategy")
FairStrategy = loadStrategyClass("octopus.dispatcher.strategies.FairStrategy")

class ControllerTestCase(unittest.TestCase):
    
    def addSimpleDispatchTree(self, fill=True, application=True, framework=True):
        self.dispatchTree = DispatchTree()
        self.user = "bud"
        if fill:
            root = self.dispatchTree.root
            jobs = FolderNode(None, "jobs", root, self.user, 1, 0, -1, FifoStrategy())
            prods = FolderNode(None, "prods", root, self.user, 1, 0, -1, FairStrategy())
            users = FolderNode(None, "users", root, self.user, 1, 0, -1, FairStrategy())
            bud = FolderNode(None, "bud", users, self.user, 1, 0, -1, FifoStrategy())
            task0 = Task(None, "bud stuff", self.user, 1, 0, "duranduboi.io.BudStuff", {"more": "beer"}, "True", [], {"beer": True})
            budtask0 = TaskNode(None, "bud stuff", bud, self.user, 1, 0, -1, task0)
            acs = FolderNode(None, "acs", users, self.user, 1, 0, -1, FifoStrategy())
            jbs = FolderNode(None, "jbs", users, self.user, 1, 0, -1, FifoStrategy())
        if application:
            self.application = Mock()
            self.application.dispatchTree=self.dispatchTree
            if framework:
                self.framework = Mock()
                self.framework.application = self.application
        elif framework:
            self.framework = Mock()
            self.framework.application.dispatchTree = self.dispatchTree
        self.framework.addOrder.return_value = sentinel.AddOrderTicket
        self.ticket = sentinel.AddOrderTicket
        sentinel.AddOrderTicket.id = 42
        sentinel.AddOrderTicket.status = Ticket.OPENED
        sentinel.AddOrderTicket.message = "fake message"
            
def suite():
    import test_nodes
    import test_rendernodes
    import test_tasks
    suites = []
    suites.append(test_rendernodes.suite())
    suites.append(test_nodes.suite())
    suites.append(test_tasks.suite())
    return unittest.TestSuite(suites) 

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
