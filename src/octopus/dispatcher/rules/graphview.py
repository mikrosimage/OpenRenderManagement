from octopus.dispatcher.model import TaskNode, FolderNode, TaskGroup
from octopus.dispatcher import rules
import logging


logger = logging.getLogger("dispatcher")


class RuleError(rules.RuleError):
    '''Base class for GraphViewBuilder related exceptions.'''
    pass


class TaskNodeHasNoChildrenError(RuleError):
    '''Raised when a GraphViewBuilder is requested to add a child node
    to a FolderNode.
    '''


class GraphViewBuilder(object):

    def __init__(self, dispatchTree, root):
        self.dispatchTree = dispatchTree
        self.root = root

    def apply(self, task):
        id = None
        name = task.name
        parent = task.parent.nodes['graph_rule'] if task.parent else self.root
        user = task.user
        priority = task.priority
        dispatchKey = task.dispatchKey
        maxRN = task.maxRN
        if isinstance(task, TaskGroup):
            strategy = task.strategy
            node = FolderNode(id, name, parent, user, priority, dispatchKey, maxRN,
                              strategy, taskGroup=task)
        else:
            node = TaskNode(None, name, parent, user, priority, dispatchKey, maxRN, task)
        task.nodes['graph_rule'] = node
        return [node]

    def processDependencies(self, dependencies):
        # TODO dependencies should be set for restricted node statutes only: DONE, ERROR and CANCELED
        for task, taskdeps in dependencies.items():
            node = task.nodes['graph_rule']
            for deptask, statuslist in taskdeps.items():
                depnode = deptask.nodes['graph_rule']
                node.addDependency(depnode,  statuslist)

    def __repr__(self):
        return "GraphViewBuilder( root=%r, dispatchTree=%r )" % (self.root, self.dispatchTree)
