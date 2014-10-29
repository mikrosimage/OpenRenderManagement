'''
Controller for the /nodes service.
'''
from octopus.core.enums.node import NODE_ERROR, NODE_CANCELED, NODE_DONE, NODE_READY
from octopus.dispatcher.model.task import TaskGroup
from octopus.core.enums.command import CMD_READY, CMD_RUNNING, CMD_CANCELED
from octopus.dispatcher.webservice import DispatcherBaseResource

import logging
import time
try:
    import simplejson as json
except ImportError:
    import json

import tornado
from tornado.web import HTTPError


logger = logging.getLogger("main.dispatcher.webservice.NodeController")

from octopus.core.communication import *
from octopus.core.framework import ResourceNotFoundError, ControllerError, queue
from octopus.dispatcher.model import FolderNode, TaskNode, Task
from octopus.dispatcher.webservice.tasks import TaskNotFoundError

from octopus.dispatcher.model.enums import NODE_STATUS

__all__ = ['NodeNotFoundError', 'NotFolderInstanceError', 'NodesResource', 'NodeResource',
           'NodeNameResource', 'NodeStatusResource', 'NodePausedResource', 'NodePriorityResource',
           'NodeDispatchKeyResource', 'NodeMaxRNResource', 'NodeStrategyResource', 'NodeChildrenResource']

DEFAULT_LIMIT = 25


class NodeNotFoundError(ResourceNotFoundError):
    '''
    Raised when a request is sent for a node that does not exist.
    '''

    def __init__(self, node, *args, **kwargs):
        ResourceNotFoundError.__init__(self, node=node, *args, **kwargs)


class NotFolderInstanceError(ControllerError):
    '''
    Raised when a folder node specific request is sent to a task node resource.
    '''
    pass

if __name__ == '__main__':
    pass


class NodesResource(DispatcherBaseResource):
    ##@queue
    def get(self):
        self.writeCallback(self.getNode(0))

    def getNode(self, nodeId):
        '''
        Builds the representation for the given node.
        '''
        try:
            node = self._findNode(nodeId)
        except NodeNotFoundError, e:
            raise Http404("Node not found. %s" % str(e))
        data = json.dumps(node.to_json())
        return data

    def _findNode(self, nodeId):
        try:
            return self.getDispatchTree().nodes[int(nodeId)]
        except KeyError:
            raise NodeNotFoundError(nodeId)


class NodeResource(NodesResource):
    ##@queue
    def get(self, nodeId):
        self.writeCallback(self.getNode(nodeId))


class NodeNameResource(NodesResource):
    ##@queue
    def put(self, nodeId):
        '''
        Pushes an order to change the name of the given node.
        Returns the ticket for this order.
        '''
        odict = self.getBodyAsJSON()
        nodeId = int(nodeId)
        nodeName = odict['name']
        node = self._findNode(nodeId)
        node.name = nodeName
        self.writeCallback("Node name set")


class NodeCancelResource(NodesResource):
    '''
    A webservice dedicated to cancelling nodes.
    It handles the process in 2 steps:
    - reset node and commands on the dispatch tree (server side)
    - send "DELETE" request to the rendernode on which a command was assigned
    The 2nd step is executed in a dedicated thread to avoir blocking tornado
    '''

    def put(self, nodeId):
        # If user action is CANCEL, we use asynchronous webservice to avoid the timeout that
        # might occur when sending requests to each render node.

        node = self._findNode(int(nodeId))
        self.interruptedRnList = []

        for cmd in node.cmdIterator():
            if cmd.status == CMD_RUNNING:
                self.interruptedRnList.append((cmd.renderNode, cmd.id))
                cmd.status = CMD_CANCELED

        if len(self.interruptedRnList) > 0:
            from threading import Thread
            t = Thread(target=self.sendCancelRequests)
            t.start()

        # logger.debug("Done updating server")
        self.writeCallback("New status has been taken into account. Change will be effective soon")
        self.finish()

    def sendCancelRequests(self):
        '''
        Send a specific request to each rendernodes.
        '''
        for rn, cmdId in self.interruptedRnList:
            # For each RN marked as having a running command
            try:
                rn.request("DELETE", "/commands/" + str(cmdId) + "/")
            except:
                logger.warning("Problem occured interruption of %s for command %s (however command has already been reseted on the server)" % (cmdId, rn))

            # Reset RN assignment to make it available for a future assignment
            rn.clearAssignment(self)

        # Clean rn list after process
        self.interruptedRnList = []


class NodeStatusResource(NodesResource):
    '''
    TOFIX: specific case for a retry all command on errors should be handled in another WS for better understanding
    '''
    @tornado.web.asynchronous
    def put(self, nodeId):
        '''
        | Pushes an order to change the status of the given node.
        | Several cases are handled in the same webservice --> need to be refactored
        | - user wants to update all commands with error status in the specified node
        | - user wants to change the status of the specified node
        | - user wants to change the status of the specified node and all of its dependencies (cascadeUpdate, default is true)

        :param nodeId: id of the node to update
        :return: the ticket for this order.
        '''

        data = self.getBodyAsJSON()

        try:
            nodeStatus = data['status']
        except:
            raise Http400('Missing entry: "status".')
        else:
            arguments = self.request.arguments
            nodeId = int(nodeId)
            node = self._findNode(nodeId)

            #
            # handles the case of retry all commands on error
            #
            if "cmdStatus" in arguments.keys() and nodeStatus == NODE_READY:
                filterfunc = lambda command: command.status in [int(s) for s in arguments['cmdStatus']]
                # get the list of commands that are in the requested status
                if isinstance(node, FolderNode):
                    root = self.dispatcher.dispatchTree.tasks[node.taskGroup.id]
                else:
                    root = self.dispatcher.dispatchTree.tasks[node.task.id]
                commands = []
                tasks = [root]
                while tasks:
                    task = tasks.pop(0)
                    if isinstance(task, TaskGroup):
                        for child in task.tasks:
                            tasks.append(child)
                    else:
                        commands += [c for c in task.commands if filterfunc(c)]
                # reset the completion of the commands and mark them as ready
                for cmd in commands:
                    # cmd.completion = 0
                    # cmd.status = CMD_READY
                    cmd.setReadyStatusAndClear()
                if commands:
                    msg = "Restarted commands %s" % ", ".join([str(cmd.id) for cmd in commands])
                else:
                    msg = "No commands were restarted."
                self.writeCallback("Done. %s" % msg)
                self.finish()
            #
            # handles the 'general' setStatus
            #
            else:
                cascadeUpdate = bool(data.get('cascade', True))
                nodeStatus = int(nodeStatus)

                if node.status in [NODE_ERROR, NODE_CANCELED, NODE_DONE] and nodeStatus == NODE_READY:
                    node.resetCompletion()

                if nodeStatus not in NODE_STATUS:
                    raise Http400("Invalid status value %r" % nodeStatus)
                elif nodeStatus == NODE_CANCELED:
                    # If user action is CANCEL, we use asynchronous webservice to avoid the timeout that
                    # might occur when sending requests to each render node.
                    self.gen = node.cmdIterator()
                    tornado.ioloop.IOLoop.instance().add_callback(self.iterOnCommands)

                    self.writeCallback("New status (CANCEL) has been taken into account. Change will be effective soon")
                else:
                    if node.setStatus(nodeStatus, cascadeUpdate):
                        self.writeCallback("Status set to %r" % nodeStatus)
                        self.finish()
                    else:
                        self.writeCallback("Status was not changed.")
                        self.finish()

    def iterOnCommands(self):
        """
        Cancel each command in a node hierarchy (command is given by a generator on the node)
        Each command might be blocked by network pb (or machine swapping) but asynchronous mecanism will
        allow other request to be treated between each command cancelation.
        """
        try:
            # Get next command in generator
            cmd = self.gen.next()
            cmd.cancel()
            tornado.ioloop.IOLoop.instance().add_callback(self.iterOnCommands)
        except StopIteration:
            self.finish()


class NodePausedResource(NodesResource):
    ##@queue
    def put(self, nodeId):
        data = self.getBodyAsJSON()
        try:
            paused = data['paused']
        except KeyError:
            raise Http400('Missing entry: "paused".')
        except TypeError:
            raise Http400('Missing entry: "paused".')
        else:
            nodeId = int(nodeId)
            node = self._findNode(nodeId)
            node.setPaused(paused)
        self.writeCallback("Paused flag changed.")


class NodePauseKillResource(NodesResource):
    ##@queue
    def put(self, nodeId):
        nodeId = int(nodeId)
        node = self._findNode(nodeId)
        if hasattr(node, "taskGroup"):
            self.pauseandkill(node.taskGroup)
        else:
            self.pauseandkill(node.task)
        for poolShare in node.poolShares:
            poolShare.allocatedRN = 0
        node.setPaused(True)

    def pauseandkill(self, task):
        if isinstance(task, TaskGroup):
            for tsk in task.tasks:
                self.pauseandkill(tsk)
        else:
            for command in task.commands:
                if command.status is CMD_RUNNING:
                    command.setReadyAndKill()


class NodePriorityResource(NodesResource):
    ##@queue
    def put(self, nodeId):
        '''
        Pushes an order to change the priority of the given node.
        Returns the ticket for this order.
        '''
        nodeId = int(nodeId)
        odict = self.getBodyAsJSON()
        priority = odict['priority']
        priority = int(priority)
        node = self._findNode(nodeId)
        node.priority = priority
        message = "Priority for node %d set to %d." % (nodeId, priority)

        self.writeCallback(message)


class NodeDispatchKeyResource(NodesResource):
    ##@queue
    def put(self, nodeId):
        '''
        Pushes an order to change the dispatch key of the given node.
        Returns the ticket for this order.
        '''
        nodeId = int(nodeId)
        odict = self.getBodyAsJSON()
        dispatchKey = odict['dispatchKey']
        dispatchKey = float(dispatchKey)
        node = self._findNode(nodeId)
        node.dispatchKey = dispatchKey
        message = "Dispatch key for node %d set to %f." % (nodeId, dispatchKey)
        self.writeCallback(message)


class NodeMaxRNResource(NodesResource):
    ##@queue
    def put(self, nodeId):
        '''
        Pushes an order to change the maxRN of the given node.
        Returns the ticket for this order.
        '''
        nodeId = int(nodeId)
        odict = self.getBodyAsJSON()
        maxRN = odict['maxRN']
        maxRN = int(maxRN)
        node = self._findNode(nodeId)
        node.maxRN = maxRN
        message = "MaxRN for node %d set to %d." % (nodeId, maxRN)
        self.writeCallback(message)


class NodeStrategyResource(NodesResource):
    ##@queue
    def put(self, nodeId):
        '''
        Pushes an order to change the strategy of the given node.
        Returns the ticket for this order.
        '''
        odict = self.getBodyAsJSON()
        strategyClassName = odict['strategy']
        node = self._findNode(nodeId)
        if not isinstance(node, FolderNode):
            raise NotFolderInstanceError('Cannot set strategy on node %d: node is not a folder.' % int(nodeId))
        from octopus.dispatcher.strategies import loadStrategyClass, StrategyImportError
        try:
            strategyClass = loadStrategyClass(strategyClassName)
        except StrategyImportError, e:
            raise Http400("Failed to set strategy: %s" % e)
        else:
            node.strategy = strategyClass()
            message = "Strategy for node %d set to %s." % (node.id, repr(strategyClassName))
        self.writeCallback(message)


class NodeUserResource(NodesResource):
    ##@queue
    def put(self, nodeId):
        '''
        Sets the user of a node.
        '''
        data = self.getBodyAsJSON()

        if 'user' not in data:
            raise HTTPError(400, 'Missing entry: "user".')
        else:
            user = data['user']

            if user == '':
                raise HTTPError(400, 'Empty value received: user can not be empty.')

            nodeId = int(nodeId)
            node = self._findNode(nodeId)
            node.user = str(user)
            self.dispatcher.dispatchTree.toModifyElements.append(node)


class NodeProdResource(NodesResource):
    ##@queue
    def put(self, nodeId):
        data = self.getBodyAsJSON()
        try:
            prod = data['prod']
        except:
            raise HTTPError(400, 'Missing entry: "prod".')
        else:
            nodeId = int(nodeId)
            node = self._findNode(nodeId)
            node.tags["prod"] = str(prod)
            self.dispatcher.dispatchTree.toModifyElements.append(node)


class NodeChildrenResource(NodesResource):
    #@queue
    def get(self, nodeId):
        '''
        Returns a HTTP response containing the list of the children of node `nodeId`.

        :Parameters:
            nodeId : int
                the id of the node from which to retrieve children
        '''
        from octopus.dispatcher.model.node import BaseNode
        node = self._findNode(nodeId)
        odict = {}
        if isinstance(node, FolderNode):
            children = node.children
        else:
            children = []
        try:
            data = self.request.arguments
        except Http400:
            data = {}

        #
        # --- filtering
        #
        if 'id' in data:
            filteredIds = [int(id) for id in data['id']]
            children = [child for child in children if child.id in filteredIds]
            for nodeId in filteredIds:
                if not any([node.id == nodeId for node in children]):
                    raise Http404("Node not found", "Node %d not found." % nodeId, "text/plain")
        if 'status' in data:
            statusList = [int(status) for status in data['status']]
            children = [child for child in children if child.status in statusList]
        if 'user' in data:
            children = [child for child in children if child.user in data['user']]
        if 'days' in data:
            children = [child for child in children if int((time.time() - child.creationTime) // 86400) <= int(data['days'][0])]
        for key in data:
            if key.startswith("tags:"):
                tag = unicode(key[5:])
                values = [unicode(v) for v in data.get(key)]
                children = [child for child in children if child.tags.get(tag) in values]
        #
        # --- sorting
        #
        if 'sortField' in data:
            sortFields = data['sortField']
            children = children[:]
            for sortField in sortFields:
                reverse = sortField.startswith('-')
                if reverse:
                    sortField = sortField[1:]
                if sortField in BaseNode.FIELDS:
                    key = lambda node: getattr(node, sortField, None)
                elif sortField.startswith('tags:'):
                    sortField = sortField[5:]
                    key = lambda node: node.tags.get(sortField, None)
                else:
                    continue
                children.sort(key=key, reverse=reverse)
        #
        # --- paging
        #
        if 'offset' in data or 'limit' in data:
            limit = int(data.get('limit', [DEFAULT_LIMIT])[-1])
            total = len(children)
            offset = data.get('offset', [0])[-1]
            if offset == 'last':
                offset = total
            else:
                offset = int(offset)
            first = offset
            last = offset + limit
            pagination = {'offset': offset, 'total': total, 'limit': limit}
            odict['pagination'] = pagination
            odict['total'] = total
            children = children[first:last]
        #
        # --- encoding
        #
        children = [childNode.to_json() for childNode in children]
        odict['children'] = children
        body = json.dumps(odict, separators=(',', ':'))
        self.set_status(200)
        self.writeCallback(body)


class NodeMaxAttemptResource(NodesResource):

    def put(self, nodeId):
        '''
        | Put a new value for the maxAttempt attribute of a task node
        | The new maxAttempt value is taken from request body, for instance : "{ maxAttempt : 10 }"

        :param nodeId: id of the task node to update
        '''

        # Get task object
        try:
            nodeId = int(nodeId)
            node = self._findNode(nodeId)
        except NodeNotFoundError:
            raise HTTPError(404, "Node not found: %d" % nodeId)

        # Get maxAttemtp from request body
        data = self.getBodyAsJSON()
        try:
            maxAttempt = int(data['maxAttempt'])
        except KeyError, e:
            raise HTTPError(400, 'Missing entry: "maxAttempt".')
        except (TypeError, ValueError), e:
            raise HTTPError(400, 'Invalid type for "maxAttempt", integer expected but %r received (error: %s)' % (data['maxAttempt'], e))

        # Update selected node and associated task
        if isinstance(node, TaskNode) or isinstance(node, FolderNode):
            result = node.setMaxAttempt(maxAttempt)
            if result is False:
                raise HTTPError(404, "Impossible to set 'maxAttempt' on node %d" % nodeId)
        else:
            raise HTTPError(404, "Invalid element selected: %r" % type(node))

        message = "Attribute maxAttempt of node %d has successfully been updated." % nodeId
        self.writeCallback(message)
