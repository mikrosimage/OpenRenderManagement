__author__ = "Olivier Derpierre "
__date__ = "Jan 11, 2010"
__version__ = (0, 2, 0)

import httplib
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

class GraphError(Exception):
    '''Raised on any invalid action in the dispatcher API.'''

class ConnectionError(Exception):
    '''Raised on any invalid connectino between edges in the graph.'''

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
                 lic="",
                 decomposer='puliclient.jobs.DefaultTaskDecomposer',
                 tags={},
                 timer=None):
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

    def decompose(self):
        if not self.decomposed:
            # print "decomposing", self.name
            self.decomposer(self)
            self.decomposed = True
        return self

    def addCommand(self, name, arguments):
        self.commands.append(Command(name, self, arguments))

    def dependsOn(self, task, statusList=[DONE]):
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

    def __init__(self,
                    name, 
                    expander=None, 
                    arguments={}, 
                    tags={}, 
                    environment={}, 
                    timer=None, 
                    priority=0, 
                    dispatchKey=0 ):
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

    def addNewTask(self, *args, **kwargs):
        """
        Creates a task and add it to the current TaskGroup.
        :return a reference on the created item
        """
        try:
            newTask = Task( *args, **kwargs )
        except GraphError,e:
            raise e

        self.tasks.append( newTask )
        newTask.arguments.parent = self.arguments
        newTask.environment.parent = self.environment

        return newTask

    def addNewTaskGroup(self, *args, **kwargs):
        """
        Creates a task group and add it to the current TaskGroup.
        :return a reference on the created item
        """
        try:
            newTaskGroup = TaskGroup( *args, **kwargs )
        except GraphError,e:
            raise e

        self.taskGroups.append( newTaskGroup )
        newTaskGroup.arguments.parent = self.arguments
        newTaskGroup.environment.parent = self.environment

        return newTaskGroup


    def expand(self, hierarchy):
        if not self.expanded:
            print "  In taskgroup: expanding ", self.name
            if self.expander:
                self.expander(self)
            self.expanded = True
        # we still need to expand/decompose
        # children (since addTask is public)
        if hierarchy:
            for task in self.tasks:
                print "    In taskgroup: decomposing ", task.name
                task.decompose()
            for taskGroup in self.taskGroups:
                print "    In taskgroup: expanding ", taskGroup.name
                taskGroup.expand(hierarchy)
        return self


    def setEnv(self, env):
        self.environment = env

    def __repr__(self):
        return "TaskNode(%r)" % str(self.name)


class Graph(object):
    """
    """
    def __init__(self, name, root=None, user=None, poolName=None, maxRN=-1):
        """
        Create a new graph object with given name and parameters.
        If root is given, it will be attached to the graph (wether it is a task or a taskgroup).
        It root is not specified a default taskgroup will be created as the root node.
        :param name: a string describing the graph
        :param root: optionnal node to be attached to the graph
        :param user: the owner of the graph
        :param poolName: a pool of rendernodes to use for this execution
        :param maxRN: a max number of concurrent rendernodes to use
        """
        self.name = unicode(name)

        if root is None:
            self.root = TaskGroup(name)
        else:
            if isinstance(root, Task) or isinstance(root, TaskGroup):
                self.root = root
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
        """
        if isinstance(pElemList,(list,tuple)):
            for node in pElemList:
                self.add( node )
        else:
            raise GraphError("Invalid node list given.")

    def add(self, pElem):
        """
        Adds a task or a taskgroup to the graph's root.
        If the graph root is a task, an error is raised
        :param pElem: the node to attach to the graph
        :return a reference to the attached node
        """
        
        if isinstance( self.root, Task) :
            raise GraphError("Graph root is a task, new task can only be added to task groups")

        if isinstance( pElem, Task) :
            self.root.addTask( pElem )
        elif isinstance( pElem, TaskGroup) :
            self.root.addTaskGroup( pElem )
        else:
            raise GraphError("Invalid element given, only tasks or taskGroups can be added.")

        return pElem

    def addNewTask(self, *args, **kwargs):
        """
        Creates a new task and attach it to the graph's root.
        If the graph root is a task, an error is raised
        :return a reference to the attached node
        """
        if isinstance( self.root, Task) :
            raise GraphError("Graph root is a task, new task can only be added to task groups")
        return self.root.addNewTask( *args, **kwargs )

    def addNewTaskGroup(self, *args, **kwargs):
        """
        Creates a new taskgroup and attach it to the graph's root.
        If the graph root is a task, an error is raised
        :return a reference to the attached node
        """
        if isinstance( self.root, Task) :
            raise GraphError("Graph root is a task, new task can only be added to task groups")
        return self.root.addNewTaskGroup( *args, **kwargs )


    def addEdges(self, pEdgeList):
        """
        Create edges to the current graph.
        Edges are given as a list of element, with each elem being a sequence of: sourceNode, destNode and status
        example edge list: [ (taskA, taskB), (taskA, taskC, [ERROR,CANCELED]), ... ]
        :param pEdgeList: List of elements indicating the source and dest node and a list of ending status (source, desti, [endStatus])
        :return a boolean indicating if the connections have been executed corretly
        """
        for (i, edge) in enumerate(pEdgeList):

            if len(edge) not in (2, 3):
                if len(edge) < 2:
                    msg = "Invalid connection for edge["+str(i)+"]="+str(edge)+": either source of destination was omitted. The edge description must at least have 2 parts"
                    raise ConnectionError(msg)
                if 3 < len(edge):
                    msg = "Invalid connection for edge["+str(i)+"]="+str(edge)+": edge description can not have more thant 3 parts."
                    raise ConnectionError(msg)

            # Edge is correct here, it has 2 or 3 elements
            if len(edge) == 2:
                # no status given, considering default result status: [DONE]
                statusList = [DONE]
            else:
                if isinstance(edge[2], list):
                    statusList = edge[2]
                else:
                    msg = "Invalid connection for edge["+str(i)+"]="+str(edge)+": statuslist is not a proper list."
                    raise ConnectionError(msg)

            # Creating link
            srcNode = edge[0]
            destNode = edge[1]
            destNode.dependencies[ srcNode ] = statusList

        return True

    def addChain(self, pEdgeChain, pEndStatusList=[DONE] ):
        """
        Create edges to the current graph.
        Edges are given as a chain of element to link as a chain, example chain: [ taskA, taskB, taskC, ... ]
        :param pEdgeList: List of elements indicating nodes to be chained in the given order
        :param pEndStatusList: A list of end status to be defined for every connections
        :return a boolean indicating if the connections have been executed corretly
        """
        if not isinstance(pEndStatusList, (list, tuple)):
            raise ConnectionError("Invalid end status given, it must be a list of statuses.")

        if not isinstance(pEdgeChain, (list, tuple)):
            raise ConnectionError("Invalid edge chain given, it must be a list of Task or TaskGroup.")

        # Loop over list by pair
        for (srcNode, destNode) in zip( pEdgeChain[:-1], pEdgeChain[1:] ):
            print ("Add dependency: %r -> %r with status in %r" % (srcNode, destNode, pEndStatusList))
            destNode.dependencies[ srcNode ] = pEndStatusList
        return True


    def toRepresentation(self):
        return GraphDumper().dumpGraph(self)


    def __repr__(self):
        """
        Returns the graph representation as JSON with a 4 spaces indentation
        """
        return json.dumps(self.toRepresentation(), indent=4)


    def submit(self, host="puliserver", port=8004):
        """
        Prepare a graph representation to be sent to the server. Several steps must be taken:
        - parse graph to resolve dependencies like task -> taskgroup
          a taskgroup cannot depend from a task, we must add the dependencies to all its tasks
        - parse graph to expand/decompose tasks and taskgroups
        - use GraphDumper class to get JSON representation
        - submit data via http
        """

        # Expand or decompose the graph hierarchically
        print "1. Expanding and decomposing hierarchy..."
        if isinstance(self.root, TaskGroup):
            self.root = self.root.expand(True)
        else:
            assert isinstance(self.root, Task)
            self.root = self.root.decompose()

        # Create JSON representation
        repr = self.toRepresentation()

        # Precompile dependencies on a taskgroup
        print "2. Checking dependencies on taskgroups..."
        for i,node in enumerate(repr["tasks"]):
            # print "    node[%d] = %s (%s)" % (i, node["name"], node["type"])
            if node["type"] is "TaskGroup" and len(node["dependencies"]) is not 0:
                # Taskgroup with a dependency
                print "  - Taskgroup %s has %d dependencies: %r" % (node["name"], len(node["dependencies"]), node["dependencies"])
                for dep in node["dependencies"]:
                    srcNodeId = dep[0]
                    srcNode = repr["tasks"][dep[0]]
                    statusList = dep[1]
                    self.addDependencyToChildrenOf( srcNodeId, srcNode, statusList, node, repr )


        print "3. Preparing submission query..."
        jsonRepr = json.dumps(repr)

        conn = httplib.HTTPConnection(host, port)
        conn.request('POST', '/graphs/', jsonRepr, {'Content-Length': len(jsonRepr)})
        response = conn.getresponse()

        print "4. Getting result: %r" % response.status
        if response.status in (200, 201):
            return response.getheader('Location'), response.read()
        else:
            raise GraphSubmissionError((response.status, response.reason))

    def addDependencyToChildrenOf(self, pDependencySrcId, pDependencySrc, pStatusList, pDependingNode, pRepr):
        """
        Used during job submission, this method will add a dependency of a particular node to all of its children.
        This is used to enforce dependency of a Taskgroup: when a taskgroup depends on another task, we ensure 
        that all tasks in the taskgroup hierarchy is really dependent of the task.
        The following transformation will occur recursively:
          -dependingNode.dependsOn( dependencySrc ) (this node is a taskgroup)
          -becomes: all children of denpendingNode.dependsOn( dependencySrc )

        :param pDependecySrcId: id of the node to which the hierarchy must depend on
        :param pDependecySrc: ref to the node to which the hierarchy must depend on
        :param pStatusList: list of statuses to wait for
        :param pDependingNode: the node (initially a taskgroup) that depends on another node
        """
        currNode = pDependingNode
        if "tasks" in currNode:
            # On a taskgroup, parse children
            for childId in currNode["tasks"]:
                childNode = pRepr["tasks"][childId]
                # print "        - children[%r]=%r" % (childId, childNode["name"])
                self.addDependencyToChildrenOf( pDependencySrcId, pDependencySrc, pStatusList, childNode, pRepr )
        else:
            # On a task, create dependency
            currNode["dependencies"].append( (pDependencySrcId, pStatusList) )
            print "    - reporting dependency: %s -> %s" % (pDependencySrc["name"], currNode["name"])
        pass
    
    

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
            raise DependencyCycleError('Detected a cycle in the dependencies of task %r: %s' % (rootNode, " -> ".join([t.name for t in cycle])))

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
            'lic': task.lic,
            'licence': task.lic,
            'tags': task.tags,
            'timer': task.timer,
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
            'timer': taskGroup.timer,
        }


def cleanEnv(env):
    env = env.copy()
    if 'HOME' in env:
        del env['HOME']
    return env
