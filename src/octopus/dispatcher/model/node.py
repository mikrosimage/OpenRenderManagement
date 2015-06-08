from time import time
from collections import defaultdict
from weakref import WeakKeyDictionary

from octopus.dispatcher.model.enums import *
from octopus.dispatcher.model import Task

from . import models

import logging
LOGGER = logging.getLogger("main.dispatcher.dispatchtree")


class NoRenderNodeAvailable(BaseException):
    '''Raised to interrupt the dispatch iteration on an entry point node.'''


class NoLicenseAvailableForTask(BaseException):
    '''Raised to interrupt the dispatch iteration on an entry point node.'''


class DependencyListField(models.Field):
    def to_json(self, node):
        return [[dep.id, statusList] for (dep, statusList) in node.dependencies]


class PoolShareDictField(models.Field):
    def to_json(self, instance):
        return [[poolShare.id, poolShare.pool.name] for poolShare in instance.poolShares.values()]


class AdditionnalPoolShareDictField(models.Field):
    def to_json(self, instance):
        return [[ps.id, ps.pool.name] for ps in instance.additionnalPoolShares.values()]


class FolderNodeChildrenField(models.Field):
    def to_json(self, instance):
        return [child.id for child in instance.children]


class BaseNode(models.Model):

    dispatcher = None

    name = models.StringField()
    parent = models.ModelField(allow_null=True)
    user = models.StringField()
    priority = models.IntegerField()
    dispatchKey = models.FloatField()
    maxRN = models.IntegerField()
    updateTime = models.FloatField()
    poolShares = PoolShareDictField()
    additionnalPoolShares = AdditionnalPoolShareDictField()
    completion = models.FloatField()
    status = models.IntegerField()
    creationTime = models.FloatField()
    startTime = models.FloatField(allow_null=True)
    updateTime = models.FloatField(allow_null=True)
    endTime = models.FloatField(allow_null=True)
    dependencies = DependencyListField()
    averageTimeByFrame = models.FloatField(allow_null=True)
    minTimeByFrame = models.FloatField(allow_null=True)
    maxTimeByFrame = models.FloatField(allow_null=True)
    timer = models.FloatField(allow_null=True)

    @property
    def tags(self):
        return {}

    def __init__(self, id, name, parent, user, priority, dispatchKey, maxRN,
                 creationTime=None, startTime=None,
                 updateTime=None, endTime=None,
                 status=NODE_READY):
        '''
        Base class for each node in dispatcher tree structure. Holds main model
        fields.

        :param id int: unique id for this node
        :param name str:  a short string describing this node
        :param parent: a FolderNode or None if this node is a root node
        :param priority int: priority value
        :param dispatchKey int: dispatchKey value
        :param maxRN int: maximum number of render nodes that can be allocated to this tree node
        :param creationTime: timestamp indicating when the node was created
        :param startTime: timestamp indicating when the node was started
        :param updateTime: timestamp indicating when the node was updated
        :param endTime: timestamp indicating when the node was ended
        :param status int: current node's status
        '''
        if not self.dispatcher:
            from octopus.dispatcher.dispatcher import Dispatcher
            self.dispatcher = Dispatcher(None)
        self.__dict__['parent'] = None
        models.Model.__init__(self)
        self.id = int(id) if id is not None else None
        self.name = str(name)
        self.parent = parent
        self.user = str(user)
        self.priority = int(priority)
        self.dispatchKey = int(dispatchKey)
        self.maxRN = int(maxRN)
        self.optimalMaxRN = 0
        self.allocatedRN = 0
        self.poolShares = WeakKeyDictionary()
        self.additionnalPoolShares = WeakKeyDictionary()
        self.completion = 1.0
        self.status = status
        self.creationTime = time() if not creationTime else creationTime
        self.startTime = startTime
        self.updateTime = updateTime
        self.endTime = endTime

        self.dependencies = []
        self.reverseDependencies = []
        self.lastDependenciesSatisfaction = False
        self.lastDependenciesSatisfactionDispatchCycle = -1
        self.readyCommandCount = 0
        self.doneCommandCount = 0
        self.commandCount = 0
        self.averageTimeByFrameList = []
        self.averageTimeByFrame = 0.0
        self.minTimeByFrame = 0.0
        self.maxTimeByFrame = 0.0
        self.timer = None

    def mainPoolShare(self):
        return self.poolShares.values()[0]

    def mainPool(self):
        return self.poolShares.keys()[0]

    def to_json(self):
        base = super(BaseNode, self).to_json()
        base["allocatedRN"] = self.allocatedRN
        base["optimalMaxRN"] = self.optimalMaxRN
        base["tags"] = self.tags.copy()
        base["readyCommandCount"] = self.readyCommandCount
        base["doneCommandCount"] = self.doneCommandCount
        base["commandCount"] = self.commandCount
        return base

    def addDependency(self, node, acceptedStatus):
        # TODO dependencies should be set for restricted node statutes only: DONE, ERROR and CANCELED

        if not acceptedStatus:
            return
        if self is node:
            # skip dependencies on oneself
            return
        self.status = NODE_BLOCKED
        val = [node, acceptedStatus]
        if not val in self.dependencies:
            self.dependencies.append(val)
            if self not in node.reverseDependencies:
                node.reverseDependencies.append(self)

    def checkDependenciesSatisfaction(self):
        # TODO dependencies should be set for restricted node statutes only: DONE, ERROR and CANCELED

        if self.dispatcher.cycle == self.lastDependenciesSatisfactionDispatchCycle:
            return self.lastDependenciesSatisfaction

        self.lastDependenciesSatisfaction = True
        self.lastDependenciesSatisfactionDispatchCycle = self.dispatcher.cycle
        for node, acceptedStatus in self.dependencies:
            if node.status not in acceptedStatus:
                self.lastDependenciesSatisfaction = False
                break
        else:
            if self.parent is not None:
                self.lastDependenciesSatisfaction = self.parent.checkDependenciesSatisfaction()

        return self.lastDependenciesSatisfaction

    def __new__(cls, *args, **kwargs):

        # Remove optional attributes for __new__ call, not supported, but the attributes are still transmitter via super hierarchy
        obj = super(BaseNode, cls).__new__(cls)
        obj._parent_value = None
        obj.invalidated = True
        return obj

    def __setattr__(self, name, value):
        if name == 'parent':
            self.setParentValue(value)
        super(BaseNode, self).__setattr__(name, value)

    def setParentValue(self, parent):
        if self.parent is parent:
                return
        if self.parent:
                self.parent.removeChild(self, False)
        if parent:
                parent.addChild(self, False)
        self.__dict__['parent'] = parent

    def dispatchIterator(self):
        raise NotImplementedError

    def updateAllocation(self):
        '''
        Called by subclasses during updateCompletion process to store maxRN and allocatedRN in the node.
        maxRN is also updated during webservice on user requests, this is a bit of a redefinition since it shouldn't change programmatically.
        '''

        # Need to iterate over all poolshares concerning the current node.
        # Otherwise we only update the allocatedRN of current pool (node the right value when user has changed pool during render)
        # nodeSharesList = [poolshare for poolshare in self.dispatcher.dispatchTree.poolShares.values() if poolshare.node.id == self.id and poolshare.node.status in [NODE_RUNNING, NODE_ERROR, NODE_PAUSED] ]
        # self.allocatedRN = 0
        # for currPoolShare in nodeSharesList:
        #     self.maxRN = currPoolShare.maxRN
        #     self.allocatedRN += currPoolShare.allocatedRN

        # Correct way, iterate over active poolshare and additionnal poolshares only
        self.allocatedRN = 0
        for currPoolShare in self.poolShares.values():
            self.maxRN = currPoolShare.maxRN
            self.allocatedRN += currPoolShare.allocatedRN

        for ps in self.additionnalPoolShares.values():
            self.allocatedRN += ps.allocatedRN

    def updateCompletionAndStatus(self):
        raise NotImplementedError

    def __repr__(self):
        nodes = [self]
        parent = self.parent
        while parent is not None:
            nodes.insert(0, parent)
            parent = parent.parent
        names = [node.name for node in nodes]
        return "<Node name='%s' path='/%s'>" % (self.name, "/".join(names))

    def __str__(self):
        return "%s: maxRN=%d allocatedRN=%d" % (self.name, self.maxRN, self.allocatedRN)

    parent_value = property(lambda self: self._parent_value, setParentValue)

    def invalidate(self):
        self.invalidated = True
        while self.parent and not self.parent.invalidated:
            self.parent.invalidated = True
            self = self.parent


class FolderNode(BaseNode):

    strategy = models.StrategyField()
    taskGroup = models.ModelField(allow_null=True)
    children = FolderNodeChildrenField()

    @property
    def tags(self):
        return self.taskGroup.tags if self.taskGroup else {}

    ##
    # @param id an integer, unique for this node
    # @param name a short string describing this folder
    # @param parent a FolderNode or None if this node is a root node
    # @param priority an integer priority value
    # @param dispatchKey a floating-point dispatchKey value
    # @param maxRN an integer value representing the maximum number of render
    #              nodes that can be allocated to this tree node.
    # @param allocator a DispatchStrategy object
    #
    def __init__(self, id, name, parent, user, priority, dispatchKey, maxRN, strategy, creationTime=None, startTime=None, updateTime=None, endTime=None, status=NODE_DONE, taskGroup=None):
        BaseNode.__init__(self, id, name, parent, user, priority, dispatchKey, maxRN, creationTime, startTime, updateTime, endTime, status)
        self.children = []
        self.strategy = strategy
        self.taskGroup = taskGroup
        if taskGroup is not None:
            self.timer = taskGroup.timer

    def addChild(self, child, setParent=True):
            if child.parent is not self and setParent:
                child.parent = self
            else:
                self.children.append(child)
                self.fireChildAddedEvent(child)

    def removeChild(self, child, setParent=True):
            if child.parent is self and setParent:
                child.parent = None
            else:
                self.children.remove(child)
                self.fireChildRemovedEvent(child)

    def fireChildAddedEvent(self, child):
        self.invalidate()
        for l in self.changeListeners:
            try:
                l.onChildAddedEvent(self, child)
            except AttributeError:
                pass

    def fireChildRemovedEvent(self, child):
        self.invalidate()
        for l in self.changeListeners:
            try:
                l.onChildRemovedEvent(self, child)
            except AttributeError:
                pass

    def cmdIterator(self):
        for child in self.children:
            for command in child.cmdIterator():
                yield command
        # if pCascadeUpdate:
        #     for dependingNode in self.reverseDependencies:
        #         dependingNode.setStatus( pStatus, pCascadeUpdate )

        # for child in self.children:
        #     child.setStatus(pStatus, pCascadeUpdate)

        # self.status = pStatus
        # return True

    ##
    # @return yields (node, command) tuples
    #
    def dispatchIterator(self, stopFunc, ep=None):
        if ep is None:
            ep = self

        if self.readyCommandCount == 0:
            return
        self.strategy.update(self, ep)

        for child in self.children:
            try:
                # PRA: only the TaskNode.dispatchIterator() may raise NoRenderNodeAvailable or NoLicenseAvailableForTask
                for assignment in child.dispatchIterator(stopFunc, ep):
                    node, command = assignment
                    self.strategy.on_assignment(self, child, node)
                    yield assignment
            # If no render node available for a command, all the commands of the parent tasks will not find a RN
            except NoRenderNodeAvailable:
                return
            # Lack of licence is specific to a command, so we continue to iterate through the graph
            except NoLicenseAvailableForTask:
                LOGGER.info("Missing license for node \"%s\" (other commands can start anyway)." % self.name)
                continue
            # We should stop if stopFunction is reached
            if stopFunc():
                return

    def updateCompletionAndStatus(self):
        """
        Evaluate new value for completion and status of a particular FolderNode
        """

        self.updateAllocation()

        if not self.invalidated:
            return
        if not self.children:
            completion = 1.0
            status = NODE_DONE
        else:

            # Getting completion info
            self.readyCommandCount = 0
            self.doneCommandCount = 0
            self.commandCount = 0
            completion = 0.0
            status = defaultdict(int)
            for child in self.children:
                child.updateCompletionAndStatus()
                completion += child.completion
                status[child.status] += 1
                self.readyCommandCount += child.readyCommandCount
                self.doneCommandCount += child.doneCommandCount
                self.commandCount += child.commandCount

            if hasattr(self, "commandCount") and int(self.commandCount) != 0:
                self.completion = self.doneCommandCount / float(self.commandCount)
            else:
                # LOGGER.warning("Warning: a folder node without \"commandCount\" value was found -> %s" % self.name  )
                self.completion = completion / len(self.children)

            # Updating node's overall status
            if NODE_PAUSED in status:
                self.status = NODE_PAUSED
            elif NODE_ERROR in status:
                self.status = NODE_ERROR
            elif NODE_RUNNING in status:
                self.status = NODE_RUNNING
            elif NODE_READY in status:
                self.status = NODE_READY
            elif NODE_BLOCKED in status:
                self.status = NODE_BLOCKED
            elif NODE_CANCELED in status:
                self.status = NODE_CANCELED
            else:
                # all commands are DONE, ensure the completion is at 1.0 (in case of failed completion update from some workers)
                self.completion = 1.0
                self.status = NODE_DONE

            # Updating timers
            times = [childNode.creationTime for childNode in self.children if childNode.creationTime is not None]
            if times:
                self.creationTime = min(times)
                if self.taskGroup and (self.taskGroup.creationTime is None or self.taskGroup.creationTime > self.creationTime):
                    self.taskGroup.creationTime = self.creationTime

            times = [childNode.startTime for childNode in self.children if childNode.startTime is not None]
            if times:
                self.startTime = min(times)
                if self.taskGroup and (self.taskGroup.startTime is None or self.taskGroup.startTime > self.startTime):
                    self.taskGroup.startTime = self.startTime

            times = [childNode.updateTime for childNode in self.children if childNode.updateTime is not None]
            if times:
                self.updateTime = max(times)
                if self.taskGroup and (self.taskGroup.updateTime is None or self.taskGroup.updateTime > self.updateTime):
                    self.taskGroup.updateTime = self.updateTime

            if isFinalNodeStatus(self.status):
                times = [childNode.endTime for childNode in self.children if childNode.endTime is not None]
                if times:
                    self.endTime = max(times)
                    if self.taskGroup and (self.taskGroup.endTime is None or
                                           self.taskGroup.endTime > self.taskGroup.endTime):
                        self.taskGroup.endTime = self.endTime
            else:
                self.endTime = None
                if self.taskGroup:
                    self.taskGroup.endTime = None
        self.invalidated = False
        if self.taskGroup:
            self.timer = self.taskGroup.timer
            # FIXME: suboptimal... lazy update someday ?
            self.taskGroup.updateStatusAndCompletion()

    def setPaused(self, paused):
        for child in self.children:
            child.setPaused(paused)

    def resetCompletion(self):
        self.completion = 0
        for child in self.children:
            child.resetCompletion()

    def setStatus(self, pStatus, pCascadeUpdate=False):
        '''
        | Propagates a target status update request.
        | @see doc/design/node-status-update.txt

        :param pStatus: New status value to assign to the current node
        :param pCascadeUpdate: Flag indicating if the depending nodes need to be updated in cascade
        '''
        if pCascadeUpdate:
            for dependingNode in self.reverseDependencies:
                dependingNode.setStatus(pStatus, pCascadeUpdate)

        for child in self.children:
            child.setStatus(pStatus, pCascadeUpdate)

        self.status = pStatus
        return True

    def setMaxAttempt(self, maxAttempt):
        '''
        '''
        globalResult = True

        for child in self.children:
            res = child.setMaxAttempt(maxAttempt)
            if res is False:
                globalResult = False

        self.dispatcher.dispatchTree.toModifyElements.append(self)
        return globalResult


class TaskNode(BaseNode):

    task = models.ModelField()
    paused = models.BooleanField()
    maxAttempt = models.IntegerField()

    @property
    def tags(self):
        if self.task is not None:
            return self.task.tags
        return None

    def __init__(self, id, name, parent, user, priority, dispatchKey, maxRN, task, creationTime=None, startTime=None, updateTime=None, endTime=None, status=NODE_BLOCKED, paused=False, maxAttempt=1):
        '''
        :param id: an integer, unique for this node
        :param name: a short string describing this folder
        :param parent: a FolderNode or None if this node is a root node
        :param priority: an integer priority value
        :param dispatchKey: a floating-point dispatchKey value
        :param maxRN: an integer value representing the maximum number of render
                      nodes that can be allocated to this tree node.
        :param task: a Task object
        '''
        BaseNode.__init__(self, id, name, parent, user, priority, dispatchKey, maxRN, creationTime, startTime, updateTime, endTime, status)
        self.task = task
        self.paused = paused
        self.maxAttempt = int(maxAttempt)

        self.commmandCount = 0

        if task is not None:
            self.timer = task.timer
            self.maxAttempt = int(task.maxAttempt)
            self.commandCount = len(task.commands)

    def cmdIterator(self):
        for command in self.task.commands:
            yield command

    def dispatchIterator(self, stopFunc, ep=None):
        # PRA : we don't use the stop function here ...

        if ep is None:
            ep = self

        # Return if no readyCommand or job in pause
        if self.readyCommandCount == 0:
            return
        if self.paused:
            return

        # ensure we are treating the commands in the order they arrived
        sorted(self.task.commands, key=lambda x: x.id)
        for command in self.task.commands:
            if command.status != CMD_READY:
                continue
            # PRA : search a render node to assign command
            renderNode = self.reserve_rendernode(command, ep)
            # PRA : renderNode is None if we did not found a RN that match job contraints
            if not renderNode:
                # PRA : Requirements depends on the task (and not the command)
                # So if we don't find a RN for a command, we would not find one for the command related to the same task
                return

            # Decrease the number of ready commands through the hierarchy
            self.readyCommandCount -= 1
            tmp_ep = ep
            while tmp_ep:
                tmp_ep.readyCommandCount -= 1
                tmp_ep = tmp_ep.parent

            yield (renderNode, command)

    def reserve_rendernode(self, command, ep):
        '''
        :param command:
        :returns: renderNode assigned to command
                  None if no RNs found due to constraints
        '''
        if ep is None:
            ep = self

        for poolshare in [poolShare for poolShare in ep.poolShares.values() if poolShare.hasRenderNodesAvailable()]:
            # first, sort the rendernodes according their performance value
            rnList = sorted(poolshare.pool.renderNodes, key=lambda rn: rn.performance, reverse=True)
            for rendernode in rnList:
                if rendernode.isAvailable() and rendernode.canRun(command):
                    if rendernode.reserveLicense(command, self.dispatcher.licenseManager):
                        rendernode.addAssignment(command)
                        return rendernode
                    else:
                        raise NoLicenseAvailableForTask

        # Might not be necessary anymore because first loop is based on poolShare's hasRNSavailable method
        # It was not taking into account the tests before assignment: RN.canRun()
        if not [poolShare for poolShare in ep.poolShares.values() if poolShare.hasRenderNodesAvailable()]:
            raise NoRenderNodeAvailable
        return None

    def updateCompletionAndStatus(self):
        '''
        Evaluate new value for completion and status of a particular TaskNode
        '''
        self.updateAllocation()

        if not self.invalidated:
            return
        if self.task is None:
            self.status = NODE_CANCELED
            return
        completion = 0.0
        status = defaultdict(int)
        self.readyCommandCount = 0
        self.doneCommandCount = 0
        self.commandCount = len(self.task.commands)

        for command in self.task.commands:
            completion += command.completion
            status[command.status] += 1
            if command.status == CMD_READY:
                self.readyCommandCount += 1
            if command.status == CMD_DONE:
                self.doneCommandCount += 1

        if self.task.commands:
            self.completion = completion / len(self.task.commands)
        else:
            self.completion = 1.0

        if CMD_CANCELED in status:
            self.status = NODE_CANCELED
        elif self.paused:
            self.status = NODE_PAUSED
        elif CMD_ERROR in status:
            self.status = NODE_ERROR
        elif CMD_TIMEOUT in status:
            self.status = NODE_ERROR
        elif CMD_RUNNING in status:
            self.status = NODE_RUNNING
        elif CMD_ASSIGNED in status:
            self.status = NODE_READY
        elif CMD_FINISHING in status:
            self.status = NODE_RUNNING
        elif CMD_READY in status:
            self.status = NODE_READY
        elif CMD_BLOCKED in status:
            self.status = NODE_BLOCKED
        else:
            # all commands are DONE, ensure the completion is at 1.0 (in case of failed completion update from some workers)
            self.completion = 1.0
            self.status = NODE_DONE

        times = [command.creationTime for command in self.task.commands if command.creationTime is not None]
        if times:
            self.creationTime = min(times)

        times = [command.startTime for command in self.task.commands if command.startTime is not None]
        if times:
            self.startTime = min(times)

        times = [command.updateTime for command in self.task.commands if command.updateTime is not None]
        if times:
            self.updateTime = max(times)

        # only set the endTime on the node if it's done
        if self.status == NODE_DONE:
            times = [command.endTime for command in self.task.commands if command.endTime is not None]
            if times:
                self.endTime = max(times)
        else:
            self.endTime = None

        self.task.status = self.status
        self.task.completion = self.completion
        self.task.creationTime = self.creationTime
        self.task.startTime = self.startTime
        self.task.updateTime = self.updateTime
        self.task.endTime = self.endTime

        self.timer = self.task.timer

        self.invalidated = False

    def checkDependenciesSatisfaction(self):
        # TODO dependencies should be set for restricted node statutes only: DONE, ERROR and CANCELED
        taskNodes = [taskNode
                     for taskNode in self.dispatcher.dispatchTree.nodes.values()
                     if isinstance(taskNode, TaskNode) and taskNode.task == self.task]
        return all(BaseNode.checkDependenciesSatisfaction(taskNode) for taskNode in taskNodes)

    def setPaused(self, paused):
        # pause every job not done
        if self.status != NODE_DONE:
            self.paused = paused
        if self.status == NODE_PAUSED and not paused:
            self.status = NODE_READY
        self.invalidate()

    def setMaxAttempt(self, maxAttempt):
        '''
        '''
        if not isinstance(self.task, Task):
            return False

        # Update node's task if exists
        self.task.maxAttempt = maxAttempt

        # Update node
        self.maxAttempt = maxAttempt

        self.dispatcher.dispatchTree.toModifyElements.append(self.task)
        self.dispatcher.dispatchTree.toModifyElements.append(self)

        return True

    def resetCompletion(self):

        self.completion = 0
        for command in self.task.commands:
            command.completion = 0

    def setStatus(self, pStatus, pCascadeUpdate=False):
        '''
        | Update commands in order to reach the required status.
        | If proper param is given, depending node will receive the same status.

        :param pStatus: New status value to assign to the current node
        :param pCascadeUpdate: Flag indicating if the depending node need to be updated in cascade
        '''

        if pCascadeUpdate:
            for dependingNode in self.reverseDependencies:
                dependingNode.setStatus(pStatus, pCascadeUpdate)

        if pStatus == NODE_CANCELED and self.status != NODE_DONE:
            for command in self.task.commands:
                command.cancel()
        elif pStatus == NODE_READY and self.status != NODE_RUNNING:
            if any(isRunningStatus(command.status) for command in self.task.commands):
                return False
            for command in self.task.commands:
                command.setReadyStatus()
        elif pStatus in (NODE_DONE, NODE_ERROR, NODE_BLOCKED, NODE_RUNNING):
            return False
        return True
