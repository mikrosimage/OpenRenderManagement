__author__ = "Olivier Derpierre <oderpierre@quintaindustries.com>"
__date__ = "Jan 11, 2010"
__version__ = (0, 2, 0)

import sys
import httplib
try:
    import json
except ImportError:
    import simplejson as json
from puliclient import jobs

__all__ = ['jobs', 'Error', 'GraphSubmissionError', 'TaskAlreadyDecomposedError', 'Task', 'Graph', 'TaskGroup', 'Command']

# -- Node status list
#
BLOCKED = 0
READY = 1
RUNNING = 2
DONE = 3
ERROR = 4
CANCELED = 5
PAUSED = 6

__all__ += ['BLOCKED', 'READY', 'RUNNING', 'DONE', 'ERROR', 'CANCELED', 'PAUSED']

# -- Submission API
#
class Error(Exception):
    '''Raised on any invalid action in the dispatcher API.'''


class HierarchicalDict(dict):

    def __init__(self, parent, args):
        dict.__init__(self, args)
        self.parent = parent

    def __getitem__(self, name):
        try:
            return dict.__getitem__(self, name)
        except KeyError:
            if self.parent:
                return self.parent[name]
            else:
                raise

    def __str__(self):
        ancestors = [self]
        parent = self.parent
        while parent:
            ancestors.append(parent)
            parent = parent.parent
        d = {}
        for ancestor in reversed(ancestors):
            d.update(ancestor)
        return str(d)


class DependencyCycleError(Error):
    '''Raised when a dependency is detected among the dependency graph.'''

class GraphSubmissionError(Error):
    '''Raised on a job submission error.'''


class TaskAlreadyDecomposedError(Error):
    '''Raised on task decomposition call on an already decomposed task.'''


class Command(object):

    def __init__(self, description, task, arguments={}):
        self.description = description
        self.task = task
        #self.arguments = HierarchicalDict(task.arguments, arguments)
        self.arguments = arguments


class Task(object):

    _decomposer = None

    def _getDecomposer(self):
        return self._decomposer

    def _setDecomposer(self, decomposer):
        self._decomposer = jobs.loadTaskDecomposer(decomposer) if decomposer else None
        self.decomposed = False
        self.commands = []

    decomposer = property(_getDecomposer, _setDecomposer)

    def __init__(self,
                 name,
                 arguments,
                 runner=None,
                 dependencies={},
                 maxRN=0,
                 priority=0,
                 dispatchKey=0,
                 environment={},
                 validator='0',
                 minNbCores=1,
                 maxNbCores=0,
                 ramUse=0,
                 requirements={},
                 licence="",
                 decomposer='puliclient.jobs.DefaultTaskDecomposer',
                 tags={}):
        self.parent = None
        self.decomposerName = decomposer
        self.decomposer = decomposer
        self.decomposed = False
        self.canBeDecomposed = True
        self.runner = runner
        self.name = unicode(name)
        self.arguments = HierarchicalDict(None, arguments)
        self.environment = HierarchicalDict(None, environment)
        self.dependencies = dependencies.copy()
        self.maxRN = maxRN
        self.priority = priority
        self.dispatchKey = dispatchKey
        self.validator = validator
        self.requirements = requirements
        self.minNbCores = minNbCores
        self.maxNbCores = maxNbCores
        self.ramUse = ramUse
        self.commands = []
        self.licence = licence
        self.tags = tags.copy()


    def decompose(self):
        if not self.decomposed:
            print "decomposing", self.name
            self.decomposer(self)
            self.decomposed = True
        return self


    def addCommand(self, name, arguments):
        self.commands.append(Command(name, self, arguments))


    def dependsOn(self, task, statusList):
        self.dependencies[task] = statusList


    def setEnv(self, env):
        self.environment = env


    def __repr__(self):
        return "Task(%r)" % str(self.name)


class TaskGroup(object):

    _expander = None

    def _getExpander(self):
        return self._expander

    def _setExpander(self, expander):
        self._expander = jobs.loadTaskExpander(expander) if expander else None
        self.expanded = False
        self.tasks = []
        self.taskGroups = []

    expander = property(_getExpander, _setExpander)


    @classmethod
    def createFromTask(cls, task):
        taskGroup = cls(task.name)
        taskGroup.arguments = task.arguments
        taskGroup.dependencies = task.dependencies
        taskGroup.environment = task.environment
        taskGroup.requirements = task.requirements
        return taskGroup


    def __init__(self, name, expander=None, arguments={}, tags={}, environment={}):
        self.expanderName = expander
        self.expander = expander
        self.expanded = False
        self.name = name
        self.arguments = HierarchicalDict(None, arguments)
        self.environment = HierarchicalDict(None, environment)
        self.requirements = {}
        self.dependencies = {}
        self.maxRN = 0
        self.priority = 0
        self.dispatchKey = 0
        self.strategy = 'octopus.dispatcher.strategies.FifoStrategy'
        self.tasks = []
        self.taskGroups = []
        self.tags = tags.copy()


    def dependsOn(self, task, statusList):
        self.dependencies[task] = statusList


    def addTask(self, task):
        assert isinstance(task, Task)
        assert not task in self.tasks
        self.tasks.append(task)
        task.arguments.parent = self.arguments
        task.environment.parent = self.environment


    def addTaskGroup(self, taskGroup):
        assert isinstance(taskGroup, TaskGroup)
        assert not taskGroup in self.taskGroups
        self.taskGroups.append(taskGroup)
        taskGroup.arguments.parent = self.arguments
        taskGroup.environment.parent = self.environment


    def expand(self, hierarchy):
        if not self.expanded:
            print "expanding", self.name
            if self.expander:
                self.expander(self)
            self.expanded = True
        # we still need to expand/decompose
        # children (since addTask is public)
        if hierarchy:
            for task in self.tasks:
                print "decomposing task", task.name
                task.decompose()
            for taskGroup in self.taskGroups:
                print "expanding task", taskGroup.name
                taskGroup.expand(hierarchy)
        return self


    def setEnv(self, env):
        self.environment = env


    def __repr__(self):
        return "TaskNode(%r)" % str(self.name)


class Graph(object):

    def __init__(self, name, root, user=None, poolName=None, maxRN=-1):
        self.name = unicode(name)
        self.root = root
        self.poolName = poolName
        self.maxRN = maxRN
        self.meta = {}
        if user is None:
            import getpass
            self.user = getpass.getuser()
        else:
            self.user = user


    def toRepresentation(self):
        return GraphDumper().dumpGraph(self)


    def submit(self, host, port):

        if isinstance(self.root, TaskGroup):
            self.root = self.root.expand(True)
        else:
            assert isinstance(self.root, Task)
            self.root = self.root.decompose()

        repr = self.toRepresentation()
        jsonRepr = json.dumps(repr)

        conn = httplib.HTTPConnection(host, port)
        conn.request('POST', '/graphs/', jsonRepr, {'Content-Length': len(jsonRepr)})
        response = conn.getresponse()
        if response.status == 201:
            return response.getheader('Location'), response.read()
        else:
            raise GraphSubmissionError((response.status, response.reason))


def _hasCycles(node, visited_nodes):
    visited_nodes = visited_nodes + [node]
    for dep in node.dependencies:
        if dep in visited_nodes:
            return visited_nodes + [dep]
        cycle = _hasCycles(dep, visited_nodes)
        if cycle:
            return cycle
    return False


class GraphDumper():

    def __init__(self):
        self.clear()


    def clear(self):
        self.tasks = []
        self.taskRepresentations = {}
        self.taskMem = {}


    def checkCycles(self, rootNode):
        cycle = _hasCycles(rootNode, [])
        if cycle:
            raise DependencyCycleError, 'Detected a cycle in the dependencies of task %r: %s' % (rootNode, " -> ".join([t.name for t in cycle]))

    def dumpGraph(self, graph):
        tasks = [graph.root]
        unvisited_tasks = [graph.root]
        while unvisited_tasks:
            task = unvisited_tasks.pop()
            tasks.append(task)
            if isinstance(task, TaskGroup):
                unvisited_tasks += [t for t in task.taskGroups if t not in tasks]
                unvisited_tasks += [t for t in task.tasks if t not in tasks]
        for task in tasks:
            self.checkCycles(task)
        self.clear()
        self.addTask(graph.root)
        repr = {
            'name': graph.name,
            'meta': graph.meta,
            'root': self.getTaskIndex(graph.root),
            'tasks': [self.taskRepresentations[task] for task in self.tasks],
            'user': graph.user,
            'poolName': graph.poolName,
            'maxRN': graph.maxRN,
        }
        return repr


    def getTaskIndex(self, task):
        try:
            return self.taskMem[task]
        except KeyError:
            return self.addTask(task)


    def addTask(self, task):
        if task in self.taskMem:
            raise Error("Attempt to send task %r twice." % task)
        self.tasks.append(task)
        taskIndex = len(self.tasks) - 1
        self.taskMem[task] = taskIndex
        if isinstance(task, Task):
            self.taskRepresentations[task] = self.computeTaskRepresentation(task)
        else:
            self.taskRepresentations[task] = self.computeTaskGroupRepresentation(task)
        return taskIndex

    def computeCommandRepresentation(self, command):
        return {
                'description': command.description,
                'type': 'command',
                'task': command.task.name,
                'arguments': command.arguments
                }

    def computeTaskRepresentation(self, task):
        return {
            'name': task.name,
            'type': 'Task',
            'runner': task.runner,
            'arguments': task.arguments,
            'environment': task.environment,
            'dependencies': [(self.getTaskIndex(dependency), statusList) for (dependency, statusList) in task.dependencies.items()],
            'maxRN': task.maxRN,
            'priority': task.priority,
            'dispatchKey': task.dispatchKey,
            'validationExpression': task.validator,
            'requirements': task.requirements,
            'minNbCores': task.minNbCores,
            'maxNbCores': task.maxNbCores,
            'ramUse': task.ramUse,
            'commands': [self.computeCommandRepresentation(command) for command in task.commands],
            'licence':task.licence,
            'tags': task.tags,
        }

    def computeTaskGroupRepresentation(self, taskGroup):
        return {
            'type': 'TaskGroup',
            'name': taskGroup.name,
            'arguments': taskGroup.arguments,
            'environment': taskGroup.environment,
            'dependencies': [(self.getTaskIndex(dependency), statusList) for (dependency, statusList) in taskGroup.dependencies.items()],
            'requirements': taskGroup.requirements,
            'maxRN': taskGroup.maxRN,
            'priority': taskGroup.priority,
            'dispatchKey': taskGroup.dispatchKey,
            'strategy': taskGroup.strategy,
            'tasks': [self.getTaskIndex(task) for task in taskGroup.tasks] + [self.getTaskIndex(subtaskGroup) for subtaskGroup in taskGroup.taskGroups],
            'tags': taskGroup.tags,
        }

def cleanEnv(env):
    env = env.copy()
    if 'HOME' in env:
        del env['HOME']
    return env
