####################################################################################################
# @file __init__.py
# @package puli.dispatcher.strategies
# @author Olivier Derpierre
# @date 2009/04/28
# @version 0.1
#
# This module exports all the basic dispatch strategies:
# - AsIsStrategy, a strategy that does nothing;
# - FifoStrategy, a strategy that sorts children according to their id;
# - FairStrategy, a strategy that shares "fairly" the allocated render nodes to the children;
# - WeighedFairStrategy, a strategy similar to the FairStrategy but giving fewer render nodes to
#                        the children with higher dispatchKeys;
# - PriorityStrategy, a strategy that gives the nodes to the children with the highest priority.
#
# To define a new strategy, you have to write a class that implements two methods:
# - update(self, folder, entrypoint) -> sorts the folder's children according to the strategy
# - on_assignment(self, folder, task, rendernode) -> called after a rendernode has been assigned
#                                                    to a child or descendant of the folder.
#
####################################################################################################

__all__ = ['loadStrategyClass', 'createStrategyInstance']

from collections import defaultdict


class BaseStrategy(object):
    '''
    Base class for folder node strategies.
    '''

    def update(self, folder, entrypoint):
        raise NotImplementedError

    def on_assignment(self, folder, task, node):
        raise NotImplementedError

    def getClassName(self):
        return self.__module__ + "." + self.__class__.__name__


class FifoStrategy(BaseStrategy):

    def update(self, folder, ep):
        folder.children.sort(key=lambda child: child.id)

    def on_assignment(self, folder, task, node):
        pass

    def __str__(self):
        return "FairStrategy"


class AsIsStrategy(BaseStrategy):

    def update(self, folder, ep):
        pass

    def on_assignment(self, folder, task, node):
        pass

    def __str__(self):
        return "AsIsStrategy"


class FairStrategy(BaseStrategy):

    def __init__(self):
        self.assignment_counts = defaultdict(int)

    def update(self, folder, ep):
        folder.children.sort(self.cmp)

    def cmp(self, x, y):
        countx = self.assignment_counts[x]
        county = self.assignment_counts[y]
        val = cmp(countx, county)
        if val == 0:
            return cmp(x.id, y.id)
        else:
            return val

    def on_assignment(self, folder, task, node):
        self.assignment_counts[task] += 1

    def __str__(self):
        return "FairStrategy"


class WeighedFairStrategy(BaseStrategy):

    def __init__(self):
        self.assignment_counts = defaultdict(int)

    def update(self, folder, ep):
        folder.children.sort(self.cmp)

    def cmp(self, x, y):
        countx = self.assignment_counts[x]
        county = self.assignment_counts[y]
        val = cmp(countx, county)
        if val == 0:
            return cmp(x.id, y.id)
        else:
            return val

    def on_assignment(self, folder, task, node):
        self.assignment_counts[task] += task.dispatchKey

    def __str__(self):
        return "WeighedFairStrategy"


class PriorityStrategy(BaseStrategy):

    def update(self, folder, ep):
        folder.children.sort(self.cmp)

    def cmp(self, x, y):
        priocmp = cmp(y.priority, x.priority)
        if priocmp:
            return priocmp
        else:
            return cmp(x.id, y.id)

    def on_assignment(self, folder, task, node):
        pass

    def __str__(self):
        return "PriorityStrategy"


class StrategyImportError(ImportError):
    """Raised when an error occurs while loading a strategy class through the loadStrategyClass function."""
    pass


def loadStrategyClass(name):
    """
    Loads the strategy class whose fully qualified name is given.

    Since this function uses __import__, the module containing the strategy must be available on sys.path.

    Example:
        ZJobFirst = octopus.core.strategies.load("duranduboi.strategies.ZJobsFirst")

    """
    try:
        moduleName, cls = name.rsplit(".", 1)
    except ValueError:
        raise StrategyImportError("Invalid strategy class name '%s'. It should be like 'some.module.StrategyClassName'." % name)

    try:
        module = __import__(moduleName, fromlist=[cls])
    except ImportError:
        raise StrategyImportError("Loading strategy class %s failed: No module '%s' on PYTHONPATH." % (name, moduleName))

    try:
        strategy = getattr(module, cls)
    except AttributeError:
        raise StrategyImportError("No such strategy '%s' defined in module '%s'." % (cls, moduleName))

    if not issubclass(strategy, BaseStrategy):
        raise StrategyImportError("%s (loaded as '%s') is not a valid Strategy." % (strategy, name))

    return strategy


def createStrategyInstance(strategyClassName):
    return loadStrategyClass(strategyClassName)()
