"""
.. module:: puliclient
    :platform: Unix
    :synopsis: API to create and submit jobs on the renderfarm

.. moduleauthor:: Olivier Derpierre

"""
__author__ = "Olivier Derpierre "
__date__ = "Jan 11, 2010"
__version__ = (0, 3, 0)

import time
import copy
import os
import sys

from datetime import datetime, timedelta
from puliclient.runner import CallableRunner
from puliclient.runner import RunnerToolkit

import httplib

try:
    import simplejson as json
except ImportError:
    import json

from puliclient import jobs

from octopus.core.enums.command import *

__all__ = ['jobs', 'Error', 'GraphSubmissionError',
           'TaskAlreadyDecomposedError', 'Task', 'Graph',
           'TaskGroup', 'Command']

# -- Node status list
#
BLOCKED = 0
READY = 1
RUNNING = 2
DONE = 3
ERROR = 4
CANCELED = 5
PAUSED = 6

__all__ += ['BLOCKED',
            'READY',
            'RUNNING',
            'DONE',
            'ERROR',
            'CANCELED',
            'PAUSED']


#
# Error relative to graph construction and submission
#
class Error(Exception):
    '''Raised on any invalid action in the dispatcher API.'''


class GraphError(Exception):
    '''Raised on any invalid action in the dispatcher API.'''


class ConnectionError(Exception):
    '''Raised on any invalid connectino between edges in the graph.'''


class DependencyCycleError(Error):
    '''Raised when a dependency is detected among the dependency graph.'''


class GraphSubmissionError(Error):
    '''Raised on a job submission error.'''


class GraphExecError(Error):
    '''Raised on a job execution error.'''


class GraphExecInterrupt(Exception):
    '''Raised on a job execution interrupt.'''


class TaskAlreadyDecomposedError(Error):
    '''Raised on task decomposition call on an already decomposed task.'''


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


def parseCallable(targetCall, name, user_args, user_kwargs):
    '''
    '''
    #
    # Check callable given
    #
    import inspect

    if not (inspect.ismethod(targetCall) or inspect.isfunction(targetCall)):
        raise GraphError("Callable must be a function or method.")

    # Common args
    callableArgs = {}
    callableArgs['sysPath'] = sys.path
    callableArgs['moduleName'] = targetCall.__module__

    # Ensure params can be serialized i.e. not object, instance or function
    try:
        callableArgs['user_args'] = json.dumps(user_args)
        callableArgs['user_kwargs'] = json.dumps(user_kwargs)
    except TypeError:
        raise GraphError("Error: invalid parameters (not JSON serializable): %s" % params)

    # Specific args
    if inspect.isfunction(targetCall):
        callableArgs['execType'] = 'function'
        callableArgs['funcName'] = targetCall.__name__
        taskName = name if name != "" else callableArgs['funcName']

    if inspect.ismethod(targetCall):
        callableArgs['execType'] = 'method'
        callableArgs['className'] = inspect.getmro(targetCall.im_class)[0].__name__
        callableArgs['methodName'] = targetCall.__name__
        taskName = name if name != "" else callableArgs['className'] + "." + callableArgs['methodName']

    return (taskName, callableArgs)


class Command(object):
    """
    | The lowest level of execution of a graph. A command is basically a process
    | to instanciate on a worker node.
    | It will use the "runner" class of its parent task for its execution.
    |
    | It consists of:
    | - a description
    | - a ref to its parent task
    | - a dict of arguments to use for execution (of the task's runner)
    |
    | A default runner can handle the following arguments:
    | - cmd: a string indicating a command line to execute
    | - timeout: a positive integer indicating the max number of second that \
                 the command can run before being interrupted
    """

    def __init__(self, description, task, arguments={}, runnerPackages=None, watcherPackages=None):
        self.description = description
        self.task = task
        self.arguments = copy.copy(arguments)
        self.runnerPackages = runnerPackages
        self.watcherPackages = watcherPackages


class Task(object):
    """
    | A node of the graph that can contain commands i.e. processes that will be
    | executed on a rendernode.
    """
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
                 runner='puliclient.jobs.DefaultCommandRunner',
                 decomposer='puliclient.jobs.DefaultTaskDecomposer',
                 runnerPackages=None,
                 watcherPackages=None,
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
                 lic="",
                 tags={},
                 timer=None,
                 maxAttempt=1):
        """
        | A task contains one or more command to be executed on the render farm.
        | The parameters that will be used to create commands (the process of
        | decomposing) are the following: arguments, runner and decomposer
        | Other params are mainly used on the Task, to handle matching and
        | dispatching on the server.

        :param name str: A simple text identifier
        :param arguments dict: a dictionnary of arguments for the command
        :param runner str: class that will be responsible for the job execution
        :param decomposer: class responsible to create commands relative to this task
        :param runnerPackages str: a string representing a list of packages to use when running a command
        :param watcherPackages str: a string representing a list of packages to use when starting command watcher i.e. used to import the runner class
        :param dependencies: a list of nodes and result status from which the current task depends on
        :param maxRN int: the maximum number of workers to assign to this task
        :param priority: deprecated
        :param dispatchKey int: indicate the priority for this task
        :param environment dict: A set of env values
        :param validator: deprecated
        :param minNbCores: deprecated
        :param maxNbCores: deprecated
        :param ramUse int: the amount of RAM in megabytes needed on a render\
            node to be assigned with a command of this task
        :param requirements: deprecated
        :param lic str: a flag indicating one or several licence token to \
            reserve for each command
        :param tags dict: user defined values
        :param timer int: a date (as a timestamp) to wait before assigning \
            commands of the current task
        :param maxAttempt int: the number of execution each command should be executed before being considered ERROR
        """

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
        self.lic = lic
        self.tags = tags.copy()
        self.timer = timer
        self.maxAttempt = maxAttempt
        self.runnerPackages = None
        self.watcherPackages = ''

        if runnerPackages:
            self.runnerPackages = runnerPackages
        else:
            self.runnerPackages = os.environ.get('REZ_USED_RESOLVE', '')

        if watcherPackages:
            self.watcherPackages = watcherPackages
        else:
            rezResolve = os.environ.get('REZ_USED_RESOLVE', '')
            for package in rezResolve.split():
                if package.startswith('pulicontrib'):
                    self.watcherPackages = "%s %s" % (self.watcherPackages, package)


    def updateTags(self, pTags):
        """
        Updates the tag dicitonnary for this element.

        :param pTags: a dict of tags (with existing or new values)
        """
        self.tags.update(pTags)

    def decompose(self):
        """
        | Call the task "decomposer", a utility class that will generate one \
        | or several command regarding decomposing method.
        | For instance given a start and end attributes, we will created a \
        | sequence of command.
        | if the task has no decomposer defined (when user manually add\
        | commands), simply "visit" the task

        :return: a ref to itself
        """
        if not self.decomposed:
            if self.decomposer is not None:
                self.decomposer(self)
            self.decomposed = True
        return self

    def addCommand(self, name, arguments, runnerPackages=None, watcherPackages=None):
        """
        Manually add a command on the current node.

        :param name: a custom name for this command
        :param arguments: dict of arguments for the specific command
        """
        self.commands.append(Command(name, self, arguments, runnerPackages, watcherPackages))

    def dependsOn(self, task, statusList=[DONE]):
        """
        Create a dependency constraint between the current node and the given \
        task at a particular status.

        :param task: a task to dependsOn
        :param statusList: a list of statuses to be reached (any of it) to \
            validate the dependency
        """
        self.dependencies[task] = statusList

    def setEnv(self, env):
        self.environment = env

    def __repr__(self):
        return "Task(%r)" % str(self.name)


class TaskGroup(object):
    """
    A node of a graph that can contain other nodes: taskgroups or tasks
    """

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
        """
        Creates a taskgroup from a task given in parameter

        :param task: task model
        :return: a new taskgroup
        """
        taskGroup = cls(task.name)
        taskGroup.arguments = task.arguments
        taskGroup.dependencies = task.dependencies
        taskGroup.environment = task.environment
        taskGroup.requirements = task.requirements
        return taskGroup

    def __init__(self,
                 name,
                 expander=None,
                 arguments={},
                 tags={},
                 environment={},
                 timer=None,
                 priority=0,
                 dispatchKey=0):
        '''
        '''
        self.expanderName = expander
        self.expander = expander
        self.expanded = False
        self.name = name
        self.arguments = HierarchicalDict(None, arguments)
        self.environment = HierarchicalDict(None, environment)
        self.requirements = {}
        self.dependencies = {}
        self.maxRN = 0
        self.priority = priority
        self.dispatchKey = dispatchKey
        self.strategy = 'octopus.dispatcher.strategies.FifoStrategy'
        self.tasks = []
        self.taskGroups = []
        self.tags = tags.copy()
        self.timer = timer

    def dependsOn(self, task, statusList=[DONE]):
        """
        | Create a dependency constraint between the current node and the given
        | task or taskGroup at a particular status.

        :param task: a Task or a TaskGroup to dependsOn
        :param statusList: a list of statuses to be reached (any of it)
            to validate the dependency
        """
        self.dependencies[task] = statusList

    def addTask(self, task):
        """
        Add the task given as parameter to the current TaskGroup.

        :param task: task object to add to the hierarchy
        """
        assert isinstance(task, Task)
        assert not task in self.tasks
        self.tasks.append(task)
        task.arguments.parent = self.arguments
        task.environment.parent = self.environment

    def addTaskGroup(self, taskGroup):
        """
        Add the taskgroup given as parameter to the current TaskGroup.

        :param taskGroup: a taskgroup to add to the hierarchy
        """

        assert isinstance(taskGroup, TaskGroup)
        assert not taskGroup in self.taskGroups
        self.taskGroups.append(taskGroup)
        taskGroup.arguments.parent = self.arguments
        taskGroup.environment.parent = self.environment

    def addNewTask(self, *args, **kwargs):
        """
        Creates a task with given args and add it to the current TaskGroup.

        :param args: standard Task arguments
        :param kwargs: keyword Task arguments
        :return: a reference on the created item
        :raise: GraphError if task creation failed
        """
        try:
            newTask = Task(*args, **kwargs)
        except GraphError, e:
            raise e

        self.tasks.append(newTask)
        newTask.arguments.parent = self.arguments
        newTask.environment.parent = self.environment

        return newTask

    def addNewCallable(self, targetCall, name="", runner="puliclient.CallableRunner", user_args=(), user_kwargs={},
                       **kwargs):
        """
        | Wraps around TaskGroup  method to add a new Task. It accepts any callable
        | to run on the renderfarm. Additionnal Task arguments can be added as keyword args
        | It uses the internal "CallableRunner" to reload arguments and execute the callable.

        :param targetCall: callable to be serialized and run on the render farm
        :param name: optionnal task name (if not defined, method or function name will be used)
        :param user_args: a list or tuple representing positionnal arguments to use with the callable
        :param user_kwargs: a dict representing keyword arguments to use with the callable
        """
        (taskName, callableArgs) = parseCallable(targetCall, name, user_args, user_kwargs)
        self.addNewTask(taskName, arguments=callableArgs, runner=runner, **kwargs)

    def addNewTaskGroup(self, *args, **kwargs):
        """
        Creates a task group with given args and add it to the TaskGroup.

        :param args: standard Task arguments
        :param kwargs: keyword Task arguments
        :return: a reference on the created item
        :raise: GraphError if task group creation failed
        """
        try:
            newTaskGroup = TaskGroup(*args, **kwargs)
        except GraphError, e:
            raise e

        self.taskGroups.append(newTaskGroup)
        newTaskGroup.arguments.parent = self.arguments
        newTaskGroup.environment.parent = self.environment

        return newTaskGroup

    def expand(self, hierarchy):
        """
        | Expands a taskgroup hierarchy.
        | - first expand itself
        | - then expand or decompose its children (taskgroup or tasks)

        :param hierarchy: the hierarchy root node
        :return: a reference to itself
        """
        if not self.expanded:
            print "    In taskgroup: expanding ", self.name
            if self.expander:
                self.expander(self)
            self.expanded = True
        # we still need to expand/decompose
        # children (since addTask is public)
        if hierarchy:
            for task in self.tasks:
                print "      In taskgroup: decomposing ", task.name
                task.decompose()
            for taskGroup in self.taskGroups:
                print "      In taskgroup: expanding ", taskGroup.name
                taskGroup.expand(hierarchy)
        return self

    def updateTags(self, pTags):
        """
        Updates the tag dicitonnary for this element.

        :param pTags: a dict of tags (with existing or new values)
        """
        self.tags.update(pTags)

    def setEnv(self, pEnv):
        """
        Sets taskgroup environment dict with the given param.

        :param pEnv: the new environment
        """
        self.environment = pEnv

    def __repr__(self):
        return "TaskNode(%r)" % str(self.name)


class Graph(object):
    """
    | Data structure to submit to Puli server.
    | It describes one or several tasks that will be executer on the renderfarm.
    """

    def __init__(self, name, root=None, user=None, poolName='default',
                 maxRN=-1, tags={}):
        """
        | Create a new graph object with given name and parameters.
        | If root is given, it will be attached to the graph (wether it is a\
            task or a taskgroup).
        | It root is not specified a default taskgroup will be created as the\
            root node.

        :param name: a string describing the graph
        :param root: optionnal node to be attached to the graph
        :param user: the owner of the graph
        :param poolName: a pool of rendernodes to use for this execution
        :param maxRN: a max number of concurrent rendernodes to use

        :raises: GraphError
        """
        self.name = unicode(name)

        if root is None:
            self.root = TaskGroup(name, tags=tags)
        else:
            if isinstance(root, Task) or isinstance(root, TaskGroup):
                self.root = root
                self.root.updateTags(tags)
            else:
                raise GraphError("Invalid root element given.")

        self.poolName = poolName
        self.maxRN = maxRN
        self.meta = {}
        if user is None:
            import getpass

            self.user = getpass.getuser()
        else:
            self.user = user

    def addList(self, pElemList):
        """
        Add a list of nodes to the graph's root.

        :param pElemList: list of nodes (task or taskgroup)
        :raise: GraphError
        """
        if isinstance(pElemList, (list, tuple)):
            for node in pElemList:
                self.add(node)
        else:
            raise GraphError("Invalid node list given.")

    def add(self, pElem):
        """
        | Adds a task or a taskgroup to the graph's root.
        | If the graph root is a task, an error is raised.

        :param pElem: the node to attach to the graph
        :type pElem: Task or TaskGroup
        :return: a reference to the attached node
        :raise: GraphError
        """
        if isinstance(self.root, Task):
            raise GraphError("Graph root is a task, new task can only be added \
                to task groups")

        if isinstance(pElem, Task):
            self.root.addTask(pElem)
        elif isinstance(pElem, TaskGroup):
            self.root.addTaskGroup(pElem)
        else:
            raise GraphError("Invalid element given, only tasks or taskGroups \
                can be added.")

        return pElem

    def addNewTask(self, *args, **kwargs):
        """
        | Creates a new task and attach it to the graph's root.
        | If the graph root is a task, an error is raised.

        :param args: standard Task arguments
        :param kwargs: keyword Task arguments
        :return: a reference to the attached node
        :raise: GraphError
        """
        if isinstance(self.root, Task):
            raise GraphError("Graph root is a task, new task can only be added \
                to task groups")
        return self.root.addNewTask(*args, **kwargs)

    def addNewTaskGroup(self, *args, **kwargs):
        """
        | Creates a new taskgroup and attach it to the graph's root.
        | If the graph root is a task, an error is raised

        :param args: standard TaskGroup arguments
        :param kwargs: keyword TaskGroup arguments
        :return: a reference to the attached node
        :raise: GraphError
        """
        if isinstance(self.root, Task):
            raise GraphError("Graph root is a task, new task can only be added \
                to task groups")
        return self.root.addNewTaskGroup(*args, **kwargs)

    # def addNewCallable(self, targetCall, *args, **kwargs):
    # '''
    #     '''
    #     task_specific_kwargs = {}
    #     # UNSUPPORTED TASK FIELDS: priority, validator, minNbCores, maxNbCores
    #     task_specific_kwargs['name'] = kwargs.pop('name', '')
    #     task_specific_kwargs['dependencies'] = kwargs.pop('dependencies', {})
    #     task_specific_kwargs['maxRN'] = kwargs.pop('maxRN', 0)
    #     task_specific_kwargs['dispatchKey'] = kwargs.pop('dispatchKey', 0)
    #     task_specific_kwargs['environment'] = kwargs.pop('environment', {})
    #     task_specific_kwargs['ramUse'] = kwargs.pop('ramUse', 0)
    #     task_specific_kwargs['requirements'] = kwargs.pop('requirements', {})
    #     task_specific_kwargs['lic'] = kwargs.pop('lic', '')
    #     task_specific_kwargs['tags'] = kwargs.pop('tags', {})
    #     task_specific_kwargs['timer'] = kwargs.pop('timer', None)
    #     task_specific_kwargs['maxAttempt'] = kwargs.pop('maxAttempt', 1)
    #     self.addNewCallableTaskRAW(targetCall, user_args=args, user_kwargs=kwargs, **task_specific_kwargs)

    def addNewCallable(self, targetCall, name="", runner="puliclient.CallableRunner", user_args=(), user_kwargs={},
                       **kwargs):
        """
        | Wraps around graph method to add a new Task. It accepts any callable
        | to run on the renderfarm. Additionnal Task arguments can be added as keyword args

        :param targetCall: callable to be serialized and run on the render farm
        :param name: optionnal task name (if not defined, method or function name will be used)
        :param user_args: a list or tuple representing formal arguments to use with the callable
        :param user_kwargs: a dict representing keyword arguments to use with the callable
        """
        (taskName, callableArgs) = parseCallable(targetCall, name, user_args, user_kwargs)
        self.addNewTask(taskName, arguments=callableArgs, runner=runner, **kwargs)

    def addEdges(self, pEdgeList):
        """
        | Create edges to the current graph.
        | Edges are given as a list of element, with each elem being a
        | sequence of: sourceNode, destNode and status
        | example edge list: [ (taskA, taskB), (taskA, taskC, [ERROR]), ... ]

        :param pEdgeList: List of elements indicating the source and dest node \
            and a list of ending status (source, desti, [endStatus])
        :return: a boolean indicating if the connections have been executed\
             corretly
        :raise: ConnectionError
        """
        for (i, edge) in enumerate(pEdgeList):

            if len(edge) not in (2, 3):
                if len(edge) < 2:
                    msg = "Invalid connection for edge[" + str(i) + "]=\
                        " + str(edge) + ": either source of destination was omitted\
                        . The edge description must at least have 2 parts"
                    raise ConnectionError(msg)
                if 3 < len(edge):
                    msg = "Invalid connection for \
                        edge[" + str(i) + "]=" + str(edge) + ": edge description can \
                        not have more thant 3 parts."
                    raise ConnectionError(msg)

            # Edge is correct here, it has 2 or 3 elements
            if len(edge) == 2:
                # no status given, considering default result status: [DONE]
                statusList = [DONE]
            else:
                if isinstance(edge[2], list):
                    statusList = edge[2]
                else:
                    msg = "Invalid connection for\
                        edge[" + str(i) + "]=" + str(edge) + ": statuslist is not \
                        a proper list."
                    raise ConnectionError(msg)

            # Creating link
            srcNode = edge[0]
            destNode = edge[1]
            destNode.dependencies[srcNode] = statusList

        return True

    def addChain(self, pEdgeChain, pEndStatusList=[DONE]):
        """
        | Create edges to the current graph.
        | Edges are given as a list of element to link as a chain.
        | Example chain: [ taskA, taskB, taskC, ... ]
        | Elements to chain can be either Tasks or TaskGroups.
        | A connection error is raised if the edge chain or status list are
        | not properly formatted.

        :param pEdgeList: List of elements indicating nodes to be chained in \
            the given order
        :param pEndStatusList: A list of end status to be defined for every\
            connections
        :return: a boolean indicating if the connections have been executed \
            corretly
        :raise: ConnectionError
        """
        if not isinstance(pEndStatusList, (list, tuple)):
            raise ConnectionError("Invalid end status given, it must be a \
                list of statuses.")

        if not isinstance(pEdgeChain, (list, tuple)):
            raise ConnectionError("Invalid edge chain given, it must be a \
                list of Task or TaskGroup.")

        # Loop over list by pair
        for (srcNode, destNode) in zip(pEdgeChain[:-1], pEdgeChain[1:]):
            print ("Add dependency: %r -> %r with status in %r"
                   % (srcNode, destNode, pEndStatusList))
            destNode.dependencies[srcNode] = pEndStatusList
        return True

    def _toRepresentation(self):
        """
        [ Creates a JSON representation of the graph using the GraphDumper
        | Utility class.

        :rtype: string
        """
        return GraphDumper().dumpGraph(self)

    def __repr__(self):
        """
        Prints the graph as JSON with a 4 spaces indentation

        :rtype: string
        """
        return json.dumps(self._toRepresentation(), indent=4)

    def prepareGraphRepresentation(self):
        """
        | Prepare a graph representation to be sent to the server or executed
        | locally. Several steps must be taken:
        | - parse graph to resolve dependencies on taskgroups
        | - parse graph to expand/decompose tasks and taskgroups
        """

        print("---------------------")
        print("Preparing graph:")
        print("---------------------")

        # Expand or decompose the graph hierarchically
        print " - Expanding and decomposing hierarchy..."
        if isinstance(self.root, TaskGroup):
            self.root = self.root.expand(True)
        else:
            assert isinstance(self.root, Task)
            self.root = self.root.decompose()

        # Create JSON representation
        repr = self._toRepresentation()

        # Precompile dependencies on a taskgroup
        print " - Checking dependencies on taskgroups..."
        for i, node in enumerate(repr["tasks"]):
            if node["type"] == "TaskGroup" and len(node["dependencies"]) != 0:
                # Taskgroup with a dependency
                print "    - Taskgroup %s has %d dependencies: %r" % (
                    node["name"], len(node["dependencies"]),
                    node["dependencies"]
                )
                for dep in node["dependencies"]:
                    srcNodeId = dep[0]
                    srcNode = repr["tasks"][dep[0]]
                    statusList = dep[1]
                    self._addDependencyToChildrenOf(
                        srcNodeId, srcNode, statusList, node, repr)

        return repr

    def save(self, filePath):
        """
            Dump the current graph into a JSON file.
        :param filePath str: the absolute path of the output file
        """
        repr = json.dumps(self._toRepresentation(), ensure_ascii=False)
        with open(filePath, 'w') as f:
            f.write(repr)

    @staticmethod
    def loadAndSubmit(filePath, host=None, port=None):
        """
            Loads a JSON file and submits it to a sever. You can use save() to \
             generate such a JSON file
        :param filePath: the absolute path of the input file
        :param host str: server name to connect to
        :param port int: server port to connect to
        :return: A tuple with the server response ie. ('SERVER_URL/nodes/Id', \
            'Graph created. Created nodes: ...')
        :raise: GraphSubmissionError
        """
        with open(filePath, 'r') as f:
            jsonRepr = f.read()
        if host is None:
            host = os.getenv('PULIHOST', 'puliserver')
        if port is None:
            port = int(os.getenv('PULIPORT', 8004))

        conn = httplib.HTTPConnection(host, port)
        conn.request(
            'POST', '/graphs/', jsonRepr, {'Content-Length': len(jsonRepr)})
        response = conn.getresponse()
        if response.status in (200, 201):
            return response.getheader('Location'), response.read()
        else:
            raise GraphSubmissionError((response.status, response.reason))

    def submit(self, host=None, port=None):
        """
        | Prepare a graph representation and send it to the server.
        | - prepare graph
        | - use GraphDumper class to serialize it into a JSON representation
        | - submit data via http

        :param host str: server name to connect to
        :param port int: server port to connect to
        :return: A tuple with the server response ie. ('SERVER_URL/nodes/Id', \
            'Graph created. Created nodes: ...')
        :raise: GraphSubmissionError
        """
        if host is None:
            host = os.getenv('PULIHOST', 'puliserver')
        if port is None:
            port = int(os.getenv('PULIPORT', 8004))

        repr = self.prepareGraphRepresentation()

        print ""
        print("---------------------")
        print "Sending graph: %s:%r" % (host, port)
        print("---------------------")
        jsonRepr = json.dumps(repr)

        conn = httplib.HTTPConnection(host, port)
        conn.request(
            'POST', '/graphs/', jsonRepr, {'Content-Length': len(jsonRepr)})
        response = conn.getresponse()

        # print "---"
        print "Upload complete, response:%r" % response.status
        print("---------------------")
        print ""

        if response.status in (200, 201):
            return response.getheader('Location'), response.read()
        else:
            raise GraphSubmissionError((response.status, response.reason))

    def execute(self):
        """
        | Prepare a graph representation to execute locally.
        | The following steps will be executed :

        ::

            1. Prepare the graph representation with GraphDumper
            2. Parse the representation to extract all commands in a single list in "id" order
               Several attribute of the task are stored with each command (taksid, end, dependencies...)
            3. While there are some "ready" command
              3.1 Parse ready commands
                    Execute command
              3.2 Parse blocked commands
                    Check dependencies and increment "nbReadyAfterCheck" counter
              3.3 If nbReadyAfterCheck == 0
                    BREAK the while loop
            4. Write summary and return

        :return: the final state of the graph
        :rtype: int
        :raise: GraphExecError
        """

        # Prepare graph
        repr = self.prepareGraphRepresentation()

        # Parse graph to create exec order list
        executionList = []

        taskId = 1
        commandId = 1
        for node in repr["tasks"]:

            if node["type"] == "TaskGroup":
                # Parse children
                # print "TG - %s" % node["name"]
                pass

            elif node["type"] == "Task":
                # print "T - %s" % node["name"]

                # Parse commands
                for command in node["commands"]:
                    # print "  C - %r" % command["description"]
                    command["execid"] = commandId
                    command["taskid"] = taskId
                    command["runner"] = node["runner"]
                    command["runnerPackages"] = node["runnerPackages"]
                    command["watcherPackages"] = node["watcherPackages"]
                    command["validationExpression"] = node["validationExpression"]
                    command["tags"] = node["tags"]
                    command["environment"] = node["environment"]

                    command["status"] = BLOCKED

                    if "dependencies" in node.keys() and 0 < len(node["dependencies"]):
                        command["dependencies"] = node["dependencies"]
                    else:
                        command["status"] = READY
                        command["dependencies"] = None

                    executionList.append(command)
                    commandId += 1

                taskId += 1

        print ""
        print("---------------------")
        print "Executing %d commands locally:" % len(executionList)
        print("---------------------")
        numDONE, numERROR, numCANCELED = 0, 0, 0
        startDate = time.time()

        # Loop while there are no ready commands left
        while True:
            readyCommands = (command for command in executionList
                             if command["status"] == READY)
            for command in readyCommands:

                # Executing node and getting result
                # Beware: we consider the command result (cmdStatus) and the
                # corresponding task result (status)
                try:
                    result = self.execNode(command)
                except GraphExecInterrupt:
                    return CANCELED

                if result in (CMD_ERROR, CMD_TIMEOUT):
                    command["status"] = ERROR
                    numERROR += 1
                elif result is CMD_CANCELED:
                    command["status"] = CANCELED
                    numCANCELED += 1
                elif result is CMD_DONE:
                    command["status"] = DONE
                    numDONE += 1
                else:
                    print "WARNING a command has ended but it final state is\
                        invalid: %r" % CMD_STATUS_NAME[self.finalState]
                    command["status"] = ERROR
                    numERROR += 1

                command["cmdStatus"] = result

            # Check dependencies
            nbReadyAfterCheck = 0
            blockedCommands = (command for command in executionList if command["status"] == BLOCKED)
            for command in blockedCommands:
                targetId = command["dependencies"][0][0]
                statuses = command["dependencies"][0][1]

                for node in executionList:
                    if node["taskid"] == targetId and node["status"] in statuses:
                        command["status"] = READY
                        nbReadyAfterCheck += 1

            if nbReadyAfterCheck == 0:
                endDate = time.time()
                elapsedTime = endDate - startDate
                print ""
                print("---------------------")
                print "FINISHED"
                print "  Commands result:"
                print "      DONE........ %d" % numDONE
                print "      ERROR....... %d" % numERROR
                print "      CANCELED.... %d" % numCANCELED
                print ""
                print "  End date:     %16s" % datetime.fromtimestamp(endDate).strftime('%b %d %H:%M:%S')
                print "  Elapsed time: %16s" % timedelta(seconds=int(elapsedTime))
                print("---------------------")
                print ""
                break

            pass  # END WHILE there are ready commands

        # Define a return status regarding the overall
        # number of errors, cancelation and succes in commands
        if numCANCELED:
            return CANCELED
        if numERROR:
            return ERROR
        return DONE

    def execNode(self, pCommand):
        """
        | Emulate the execution of a command on a worker node.
        | 2 possible execution mode: with a subprocess or direct
        | - Calls the "commandwatcher.py" script used by the worker \
            process to keep a similar behaviour
        |   Command output and error messages are left in stdout/stderr to \
            give the user a proper feedback of its command
        | - Create CommandWatcherObject in current exec

        :param pCommand: dict containing the command description and arguments
        :raise: GraphExecInterrupt when a keyboard interrupt is raised
        """
        print ""
        #from octopus.commandwatcher import commandwatcher

        commandId = pCommand["execid"]
        # taskId = pCommand["taskid"]
        runner = pCommand["runner"]
        runnerPackages = pCommand.get("runnerPackages", "undefined")
        validationExpression = pCommand["validationExpression"]

        print(pCommand)
        #
        # SECURE WAY: start a subprocess
        #
        #####
        # CommandWatcher call, arguments expected are:
        # - python executable
        # - flag "u" to load commands from a file
        # - file to load
        # - a communication port to contact a worker if execution is done in remote contact (executed via puli's worker)
        # - a command id
        # - a runner class
        # - an optionnal validation expression

        # from octopus.commandwatcher import commandwatcher
        # scriptFile = commandwatcher.__file__
        # pythonExecutable = sys.executable
        # args = [
        #     pythonExecutable,
        #     "-u",
        #     scriptFile,
        #     "",
        #     "0",
        #     str(commandId),
        #     runner,
        #     validationExpression,
        # ]
        # args.extend(('%s=%s' % (str(name), str(value)) for (name, value) in pCommand["arguments"].items()))
        # #### normalize environment~-> TOCHECK peut etre a supprimer justement pour garder l'env en execution locale (attention au
        # #### call subprocess qui ajoute envN
        # envN = {}
        # envN["PYTHONPATH"] = "/s/apps/lin/vfx_test_apps/OpenRenderManagement/Puli/src"
        # for key in pCommand["environment"]:
        #     envN[str(key)] = str(pCommand["environment"][key])

        # # print("Starting subprocess, log: %r, args: %r" % (logFile, args) )
        # try:
        #     proc = subprocess.Popen(args, bufsize=-1, stdin=None, stdout=None,
        #                stderr=None, close_fds=True,
        #                env=envN)
        #     proc.wait()
        # except Exception,e:
        #     print("Impossible to start subprocess: %r" % e)
        #     raise e
        # except KeyboardInterrupt:
        #     sys.exit(0)
        # print ""

        #
        # DIRECT CALL: create CommandWatcher object
        #
        from octopus.commandwatcher.commandwatcher import CommandWatcher

        try:
            result = CommandWatcher("",
                                    "0",
                                    commandId,
                                    runner,
                                    runnerPackages,
                                    validationExpression,
                                    pCommand["arguments"])
            return result.finalState

        except KeyboardInterrupt:
            print("\n")
            print("Exit event caught: exiting CommandWatcher...\n")
            # sys.exit(0)
            raise GraphExecInterrupt

    def updateCompletion(self, pCompletion):
        print("Completion set: %r" % pCompletion)

    def updateMessage(self, pMessage):
        print("Message set: %r" % pMessage)

    def _addDependencyToChildrenOf(
            self, pDependencySrcId, pDependencySrc,
            pStatusList, pDependingNode, pRepr):
        """
        | Used during job submission, this method will add a dependency of a
        | particular node to all of its children.
        | This is used to enforce dependency of a Taskgroup: when a taskgroup
        | depends on another task, we ensure that all tasks in the taskgroup
        | hierarchy is really dependent of the task.
        |
        | The following transformation will occur recursively:
        | - dependingNode.dependsOn( dependencySrc ) (this node is a taskgroup)
        | - becomes: all children of denpendingNode.dependsOn( dependencySrc )

        :param pDependecySrcId: id of the node to which the hierarchy must \
            depend on
        :param pDependecySrc: reference to the node to which the hierarchy \
            must depend on
        :param pStatusList: list of statuses to wait for
        :param pDependingNode: the node (initially a taskgroup) that depends \
            on another node
        """
        currNode = pDependingNode
        if "tasks" in currNode:
            # On a taskgroup, parse children
            for childId in currNode["tasks"]:
                childNode = pRepr["tasks"][childId]
                self._addDependencyToChildrenOf(
                    pDependencySrcId, pDependencySrc,
                    pStatusList, childNode, pRepr)
        else:
            # On a task, create dependency
            currNode["dependencies"].append((pDependencySrcId, pStatusList))
            print "    - reporting dependency: %s -> %s" % (pDependencySrc["name"], currNode["name"])


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
    """
    | Processes a graph to create a serialized json to send to the server.
    | During the process, some validity checks can be done: no cycle,
    | consistency, etc.
    """

    def __init__(self):
        self.clear()

    def clear(self):
        self.tasks = []
        self.taskRepresentations = {}
        self.taskMem = {}

    def checkCycles(self, rootNode):
        cycle = _hasCycles(rootNode, [])
        if cycle:
            raise DependencyCycleError(
                'Detected a cycle in the dependencies of task %r: %s'
                % (rootNode, " -> ".join([t.name for t in cycle])))

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
        # FIXME JSA: conventionnal shot name is stored in tags{plan:name}
        # We want to change this behaviour to store it in tags:{shot:name}
        # So we hack the task repr to set the same value for both keys
        # (to keep a retrocompatibility)
        if ("shot" not in task.tags) and ("plan" in task.tags):
            task.tags["shot"] = task.tags["plan"]
        if ("plan" not in task.tags) and ("shot" in task.tags):
            task.tags["plan"] = task.tags["shot"]

        return {
            'name': task.name,
            'type': 'Task',
            'runner': task.runner,
            'arguments': task.arguments,
            'environment': task.environment,
            'dependencies': [(self.getTaskIndex(dependency), statusList) for (dependency, statusList) in
                             task.dependencies.items()],
            'maxRN': task.maxRN,
            'priority': task.priority,
            'dispatchKey': task.dispatchKey,
            'validationExpression': task.validator,
            'requirements': task.requirements,
            'minNbCores': task.minNbCores,
            'maxNbCores': task.maxNbCores,
            'ramUse': task.ramUse,
            'commands': [self.computeCommandRepresentation(command) for command in task.commands],
            'lic': task.lic,
            'licence': task.lic,
            'tags': task.tags,
            'timer': task.timer,
            'maxAttempt': task.maxAttempt,
            'runnerPackages': task.runnerPackages,
            'watcherPackages': task.watcherPackages,
        }

    def computeTaskGroupRepresentation(self, taskGroup):

        # FIXME JSA: conventionnal shot name is stored in tags{plan:name}
        # We want to change this behaviour to store it in tags:{shot:name}
        # So we hack the task repr to set the same value for both keys (to keep a retrocompatibility)
        if ("shot" not in taskGroup.tags) and ("plan" in taskGroup.tags):
            taskGroup.tags["shot"] = taskGroup.tags["plan"]
        if ("plan" not in taskGroup.tags) and ("shot" in taskGroup.tags):
            taskGroup.tags["plan"] = taskGroup.tags["shot"]

        return {
            'type': 'TaskGroup',
            'name': taskGroup.name,
            'arguments': taskGroup.arguments,
            'environment': taskGroup.environment,
            'dependencies': [(self.getTaskIndex(dependency), statusList) for (dependency, statusList) in
                             taskGroup.dependencies.items()],
            'requirements': taskGroup.requirements,
            'maxRN': taskGroup.maxRN,
            'priority': taskGroup.priority,
            'dispatchKey': taskGroup.dispatchKey,
            'strategy': taskGroup.strategy,
            'tasks': [self.getTaskIndex(task) for task in taskGroup.tasks] + [self.getTaskIndex(subtaskGroup) for
                                                                              subtaskGroup in taskGroup.taskGroups],
            'tags': taskGroup.tags,
            'timer': taskGroup.timer,
        }


def cleanEnv(env):
    env = env.copy()
    if 'HOME' in env:
        del env['HOME']
    return env
