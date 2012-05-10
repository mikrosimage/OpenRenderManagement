# coding: utf8

import logging
from weakref import WeakValueDictionary


from octopus.dispatcher.model import FolderNode, TaskNode, Pool, RenderNode, Task, TaskGroup, Command, PoolShare
from octopus.dispatcher.model.node import BaseNode
from octopus.dispatcher.strategies import FifoStrategy, loadStrategyClass
from octopus.core.enums.command import *
from octopus.dispatcher.rules import RuleError


logger = logging.getLogger("dispatcher.dispatchtree")


def splitpath(path):
    import urllib, posixpath

    path = urllib.unquote(path)
    path = path.decode("UTF-8")
    elements = []
    while True:
        path, basename = posixpath.split(path)
        if basename:
                elements.insert(0, basename)
        if path == "" or path == "/":
                break
    return elements


class ObjectListener(object):

    def __init__(self, onCreationEvent=lambda obj, field: None, onDestructionEvent=lambda obj, field: None, onChangeEvent=lambda obj, field, oldvalue, newvalue: None):
        self.onCreationEvent = onCreationEvent
        self.onDestructionEvent = onDestructionEvent
        self.onChangeEvent = onChangeEvent


class DispatchTree(object):

    def __init__(self):
        # core data
        self.root = FolderNode(0, "root", None, "root", 1, 1, 0, FifoStrategy())
        self.nodes = WeakValueDictionary()
        self.nodes[0] = self.root
        self.pools = {}
        self.renderNodes = {}
        self.tasks = {}
        self.rules = []
        self.poolShares = {}
        self.commands = {}
        # deduced properties
        self.nodeMaxId = 0
        self.poolMaxId = 0
        self.renderNodeMaxId = 0
        self.taskMaxId = 0
        self.commandMaxId = 0
        self.poolShareMaxId = 0
        self.toCreateElements = []
        self.toModifyElements = []
        self.toArchiveElements = []
        # listeners
        self.nodeListener = ObjectListener(self.onNodeCreation, self.onNodeDestruction, self.onNodeChange)
        self.taskListener = ObjectListener(self.onTaskCreation, self.onTaskDestruction, self.onTaskChange)
        self.renderNodeListener = ObjectListener(self.onRenderNodeCreation, self.onRenderNodeDestruction, self.onRenderNodeChange)
        self.poolListener = ObjectListener(self.onPoolCreation, self.onPoolDestruction, self.onPoolChange)
        self.commandListener = ObjectListener(onCreationEvent=self.onCommandCreation, onChangeEvent=self.onCommandChange)
        self.poolShareListener = ObjectListener(self.onPoolShareCreation)
        self.modifiedNodes = []

    def registerModelListeners(self):
        BaseNode.changeListeners.append(self.nodeListener)
        Task.changeListeners.append(self.taskListener)
        TaskGroup.changeListeners.append(self.taskListener)
        RenderNode.changeListeners.append(self.renderNodeListener)
        Pool.changeListeners.append(self.poolListener)
        Command.changeListeners.append(self.commandListener)
        PoolShare.changeListeners.append(self.poolShareListener)


    def destroy(self):
        BaseNode.changeListeners.remove(self.nodeListener)
        Task.changeListeners.remove(self.taskListener)
        RenderNode.changeListeners.remove(self.renderNodeListener)
        Pool.changeListeners.remove(self.poolListener)
        Command.changeListeners.remove(self.commandListener)
        PoolShare.changeListeners.remove(self.poolShareListener)
        self.root = None
        self.nodes.clear()
        self.pools.clear()
        self.renderNodes.clear()
        self.tasks.clear()
        self.rules = None
        self.commands.clear()
        self.poolShares = None
        self.modifiedNodes = None
        self.toCreateElements = None
        self.toModifyElements = None
        self.toArchiveElements = None

    def findNodeByPath(self, path, default=None):
        nodenames = splitpath(path)
        node = self.root
        for name in nodenames:
            for child in node.children:
                if child.name == name:
                    node = child
                    break
            else:
                return default
        return node


    def updateCompletionAndStatus(self):
        self.root.updateCompletionAndStatus()


    def validateDependencies(self):
        nodes = set()
        for dependency in self.modifiedNodes:
            for node in dependency.reverseDependencies:
                nodes.add(node)
        del self.modifiedNodes[:]
        for node in nodes:
            if isinstance(node, TaskNode):
                if node.checkDependenciesSatisfaction():
                    for cmd in node.task.commands:
                        if cmd.status == CMD_BLOCKED:
                            cmd.status = CMD_READY
                else:
                    for cmd in node.task.commands:
                        if cmd.status == CMD_READY:
                            cmd.status = CMD_BLOCKED


    def registerNewGraph(self, graph):
        user = graph['user']
        taskDefs = graph['tasks']
        poolName = graph['poolName']
        if 'maxRN' in graph.items():
            maxRN = int(graph['maxRN'])
        else:
            maxRN = -1
        
        #
        # Create objects.
        #
        tasks = [None for i in xrange(len(taskDefs))]
        for (index, taskDef) in enumerate(taskDefs):
            if taskDef['type'] == 'Task':
                task = self._createTaskFromJSON(taskDef, user)
            elif taskDef['type'] == 'TaskGroup':
                task = self._createTaskGroupFromJSON(taskDef, user)
            tasks[index] = task
        root = tasks[graph['root']]
        
        # get the pool
        try:
            pool = self.pools[poolName]
        except KeyError:
            pool = Pool(None, poolName)
            self.pools[poolName] = pool
        #
        # Rebuild full job hierarchy
        #
        for (taskDef, task) in zip(taskDefs, tasks):
            if taskDef['type'] == 'TaskGroup':
                for taskIndex in taskDef['tasks']:
                    task.addTask(tasks[taskIndex])
                    tasks[taskIndex].parent = task
        #
        # Compute dependencies for each created task or taskgroup object.
        #
        dependencies = {}
        for (taskDef, task) in zip(taskDefs, tasks):
            taskDependencies = {}
            if not isinstance(taskDef['dependencies'], list):
                raise SyntaxError, "Dependencies must be a list of (taskId, [status-list]), got %r." % taskDef['dependencies']
            if not all(((isinstance(i, int) and
                         isinstance(sl, list) and
                         all((isinstance(s, int) for s in sl))) for (i, sl) in taskDef['dependencies'])):
                raise SyntaxError, "Dependencies must be a list of (taskId, [status-list]), got %r." % taskDef['dependencies']
            for (taskIndex, statusList) in taskDef['dependencies']:
                taskDependencies[tasks[taskIndex]] = statusList
            dependencies[task] = taskDependencies
        #
        # Apply rules to generate dispatch tree nodes.
        #
        if not self.rules:
            logger.warning("graph submitted but no rule has been defined")

        unprocessedTasks = [root]
        nodes = []
        while unprocessedTasks:
            unprocessedTask = unprocessedTasks.pop(0)
            for rule in self.rules:
                try:
                    nodes += rule.apply(unprocessedTask)
                except RuleError:
                    logger.warning("rule %s failed for graph %s" % (rule, graph))
                    raise
            if isinstance(unprocessedTask, TaskGroup):
                for task in unprocessedTask:
                    unprocessedTasks.append(task)

        # create the poolshare, if any, and affect it to the node
        if pool:
            # FIXME nodes[0] may not be the root node of the graph...
            PoolShare(None, pool, nodes[0], maxRN)

        #
        # Process dependencies
        #
        for rule in self.rules:
            rule.processDependencies(dependencies)

        for node in nodes:
            assert isinstance(node.id, int)
            self.nodes[node.id] = node

        return nodes


    def _createTaskGroupFromJSON(self, taskGroupDefinition, user):
        # name, parent, arguments, environment, priority, dispatchKey, strategy
        id = None
        name = taskGroupDefinition['name']
        parent = None
        arguments = taskGroupDefinition['arguments']
        environment = taskGroupDefinition['environment']
        requirements = taskGroupDefinition['requirements']
        maxRN = taskGroupDefinition['maxRN']
        priority = taskGroupDefinition['priority']
        dispatchKey = taskGroupDefinition['dispatchKey']
        strategy = taskGroupDefinition['strategy']
        strategy = loadStrategyClass(strategy.encode())
        strategy = strategy()
        tags = taskGroupDefinition['tags']
        return TaskGroup(id, name, parent, user, arguments, environment, requirements,
                         maxRN, priority, dispatchKey, strategy, tags=tags)


    def _createTaskFromJSON(self, taskDefinition, user):
        # id, name, parent, user, priority, dispatchKey, runner, arguments,
        # validationExpression, commands, requirements=[], minNbCores=1,
        # maxNbCores=0, ramUse=0, environment={}
        name = taskDefinition['name']
        runner = taskDefinition['runner']
        arguments = taskDefinition['arguments']
        environment = taskDefinition['environment']
        requirements = taskDefinition['requirements']
        maxRN = taskDefinition['maxRN']
        priority = taskDefinition['priority']
        dispatchKey = taskDefinition['dispatchKey']
        validationExpression = taskDefinition['validationExpression']
        minNbCores = taskDefinition['minNbCores']
        maxNbCores = taskDefinition['maxNbCores']
        ramUse = taskDefinition['ramUse']
        lic = taskDefinition['lic']
        tags = taskDefinition['tags']
        task = Task(None, name, None, user, maxRN, priority, dispatchKey, runner,
                    arguments, validationExpression, [], requirements, minNbCores,
                    maxNbCores, ramUse, environment, lic=lic, tags=tags)

        for commandDef in taskDefinition['commands']:
            description = commandDef['description']
            arguments = commandDef['arguments']
            task.commands.append(Command(None, description, task, arguments))

        return task


    ## Resets the lists of elements to create or update in the database.
    #
    def resetDbElements(self):
        self.toCreateElements = []
        self.toModifyElements = []
        self.toArchiveElements = []


    ## Recalculates the max ids of all elements. Generally called after a reload from db.
    #
    def recomputeMaxIds(self):
        self.nodeMaxId = max([n.id for n in self.nodes.values()]) if self.nodes else 0
        self.poolMaxId = max([p.id for p in self.pools.values()]) if self.pools else 0
        self.renderNodeMaxId = max([rn.id for rn in self.renderNodes.values()]) if self.renderNodes else 0
        self.taskMaxId = max([t.id for t in self.tasks.values()]) if self.tasks else 0
        self.commandMaxId = max([c.id for c in self.commands.values()]) if self.commands else 0
        self.poolShareMaxId = max([ps.id for ps in self.poolShares.values()]) if self.poolShares else 0


    ## Removes from the dispatchtree the provided element and all its parents and children.
    #
    def unregisterElementsFromTree(self, element):
        # /////////////// Handling of the Task
        if isinstance(element, Task):
            del self.tasks[element.id]
            self.toArchiveElements.append(element)
            for cmd in element.commands:
                self.unregisterElementsFromTree(cmd)
            for node in element.nodes.values():
                self.unregisterElementsFromTree(node)
        # /////////////// Handling of the TaskGroup
        elif isinstance(element, TaskGroup):
            del self.tasks[element.id]
            self.toArchiveElements.append(element)
            for task in element.tasks:
                self.unregisterElementsFromTree(task)
            for node in element.nodes.values():
                self.unregisterElementsFromTree(node)
        # /////////////// Handling of the TaskNode
        elif isinstance(element, TaskNode):
            # remove the element from the children of the parent
            if element.parent:
                element.parent.removeChild(element)
            if element.poolShares:
                for poolShare in element.poolShares.values():
                    self.toArchiveElements.append(poolShare)
            del self.nodes[element.id]
            self.toArchiveElements.append(element)
            for dependency in element.dependencies:
                self.unregisterElementsFromTree(dependency)
        # /////////////// Handling of the FolderNode
        elif isinstance(element, FolderNode):
            if element.parent:
                element.parent.removeChild(element)
            if element.poolShares:
                for poolShare in element.poolShares.values():
                    self.toArchiveElements.append(poolShare)
            del self.nodes[element.id]
            self.toArchiveElements.append(element)
            for dependency in element.dependencies:
                self.unregisterElementsFromTree(dependency)
        # /////////////// Handling of the Command
        elif isinstance(element, Command):
            del self.commands[element.id]
            self.toArchiveElements.append(element)


    ### methods called after interaction with a Task

    def onTaskCreation(self, task):
        if task.id == None:
            self.taskMaxId += 1
            task.id = self.taskMaxId
            self.toCreateElements.append(task)
        else:
            self.taskMaxId = max(self.taskMaxId, task.id)
        self.tasks[task.id] = task

    def onTaskDestruction(self, task):
        self.unregisterElementsFromTree(task)

    def onTaskChange(self, task, field, oldvalue, newvalue):
        self.toModifyElements.append(task)

    ### methods called after interaction with a BaseNode

    def onNodeCreation(self, node):
        if node.id == None:
            self.nodeMaxId += 1
            node.id = self.nodeMaxId
            self.toCreateElements.append(node)
        else:
            self.nodeMaxId = max(self.nodeMaxId, node.id)
        if node.parent == None:
            node.parent = self.root

    def onNodeDestruction(self, node):
        del self.nodes[node.id]

    def onNodeChange(self, node, field, oldvalue, newvalue):
        # FIXME: do something when nodes are reparented from or to the root node
        if node.id is not None:
            self.toModifyElements.append(node)
            if field == "status" and node.reverseDependencies:
                self.modifiedNodes.append(node)

    ### methods called after interaction with a RenderNode

    def onRenderNodeCreation(self, renderNode):
        if renderNode.id == None:
            self.renderNodeMaxId += 1
            renderNode.id = self.renderNodeMaxId
            self.toCreateElements.append(renderNode)
        else:
            self.renderNodeMaxId = max(self.renderNodeMaxId, renderNode.id)
        self.renderNodes[renderNode.name] = renderNode

    def onRenderNodeDestruction(self, rendernode):
        del self.renderNodes[rendernode.name]
        self.toArchiveElements.append(rendernode)

    def onRenderNodeChange(self, rendernode, field, oldvalue, newvalue):
        self.toModifyElements.append(rendernode)

    ### methods called after interaction with a Pool

    def onPoolCreation(self, pool):
        if pool.id == None:
            self.poolMaxId += 1
            pool.id = self.poolMaxId
            self.toCreateElements.append(pool)
        else:
            self.poolMaxId = max(self.poolMaxId, pool.id)
        self.pools[pool.name] = pool

    def onPoolDestruction(self, pool):
        del self.pools[pool.name]
        self.toArchiveElements.append(pool)

    def onPoolChange(self, pool, field, oldvalue, newvalue):
        if pool not in self.toModifyElements:
            self.toModifyElements.append(pool)

    ### methods called after interaction with a Command

    def onCommandCreation(self, command):
        if command.id is None:
            self.commandMaxId += 1
            command.id = self.commandMaxId
            self.toCreateElements.append(command)
        else:
            self.commandMaxId = max(self.commandMaxId, command.id)
        self.commands[command.id] = command

    def onCommandChange(self, command, field, oldvalue, newvalue):
        self.toModifyElements.append(command)
        for node in command.task.nodes.values():
            node.invalidate()

    ### methods called after interaction with a Pool

    def onPoolShareCreation(self, poolShare):
        if poolShare.id is None:
            self.poolShareMaxId += 1
            poolShare.id = self.poolShareMaxId
            self.toCreateElements.append(poolShare)
        else:
            self.poolShareMaxId = max(self.poolShareMaxId, poolShare.id)
        self.poolShares[poolShare.id] = poolShare

