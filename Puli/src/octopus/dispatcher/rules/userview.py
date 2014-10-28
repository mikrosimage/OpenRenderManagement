from octopus.dispatcher.model import TaskNode, FolderNode, TaskGroup
from octopus.dispatcher import rules
from octopus.dispatcher.strategies import FifoStrategy
import logging


logger = logging.getLogger("main.userview")

RULENAME = 'UserView'


class RuleError(rules.RuleError):
    '''Base class for the UserView related exceptions.'''
    pass


class UserView(object):

    def __init__(self, dispatchTree, root):
        self.dispatchTree = dispatchTree
        self.root = root

    def apply(self, task):
        nodeId = None
        name = task.name
        parent, created = self.getOrCreateParentNode(task)
        user = task.user
        priority = task.priority
        dispatchKey = task.dispatchKey
        maxRN = task.maxRN
        if isinstance(task, TaskGroup):
            strategy = task.strategy
            node = FolderNode(nodeId, name, parent, user, priority, dispatchKey, maxRN,
                              strategy, taskGroup=task)
        else:
            node = TaskNode(None, name, parent, user, priority, dispatchKey, maxRN, task)
        task.nodes[RULENAME] = node
        if created:
            return [node.parent, node]
        else:
            return [node]

    def getOrCreateParentNode(self, task):
        if task.parent:
            return (task.parent.nodes[RULENAME], False)
        userName = task.user
        for child in self.root.children:
            if child.name == userName:
                return (child, False)
        userNode = FolderNode(None, userName, self.root, userName, 1, 1.0, -1, FifoStrategy(), None)
        return (userNode, True)

    def processDependencies(self, dependencies):
        pass

    @classmethod
    def register(cls, dispatchTree, userName, rootName):
        rootNode = FolderNode(None, rootName, dispatchTree.root, userName, 1, 1, 0, FifoStrategy(), None)
        rule = cls(dispatchTree, rootNode)
        dispatchTree.rules.append(rule)
        for task in dispatchTree.tasks.values():
            rule.apply(task)
        return rule
