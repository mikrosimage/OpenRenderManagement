'''
Controller for the /nodes service.
'''
from octopus.core.enums.node import NODE_ERROR, NODE_CANCELED, NODE_DONE, NODE_READY
from octopus.dispatcher.model.task import TaskGroup
from octopus.core.enums.command import CMD_READY, CMD_RUNNING

import logging
import time
import json

logger = logging.getLogger("dispatcher.webservice.NodeController")

from octopus.core.communication import *
from octopus.core.framework import ResourceNotFoundError, ControllerError,\
    BaseResource, queue
from octopus.dispatcher.model import FolderNode

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


class NodesResource(BaseResource):
    @queue
    def get(self):
        self.writeCallback(self.getNode(0))

    def getNode(self, nodeId):
        '''
        Builds the representation for the given node.
        '''
        try:
            node = self._findNode(nodeId)
        except NodeNotFoundError, e:
            return Http404("Node not found. %s" % str(e))
        data = json.dumps(node.to_json())
        return data

    def _findNode(self, nodeId):
        try:
            return self.getDispatchTree().nodes[int(nodeId)]
        except KeyError:
            raise NodeNotFoundError(nodeId)


class NodeResource(NodesResource):
    @queue
    def get(self, nodeId):
        self.writeCallback(self.getNode(nodeId))


class NodeNameResource(NodesResource):
    @queue
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


class NodeStatusResource(NodesResource):
    @queue
    def put(self, nodeId):
        '''
        Pushes an order to change the status of the given node.
        Returns the ticket for this order.
        '''
        data = self.getBodyAsJSON()
        try:
            nodeStatus = data['status']
        except:
            return Http400('Missing entry: "status".')
        else:
            arguments = self.request.arguments
            nodeId = int(nodeId)
            node = self._findNode(nodeId)
            # TODO handle the case when there is no node with nodeId

            # handles the case of retry all commands on error
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
                    cmd.completion = 0
                    cmd.status = CMD_READY
                if commands:
                    msg = "Restarted commands %s" % ", ".join([str(cmd.id) for cmd in commands])
                else:
                    msg = "No command were restarted."
                self.writeCallback("Done. %s" % msg)
            # handles the 'general' setStatus
            else:
                nodeStatus = int(nodeStatus)
                if node.status in [NODE_ERROR, NODE_CANCELED, NODE_DONE] and nodeStatus == NODE_READY:
                    node.resetCompletion()
                if nodeStatus not in NODE_STATUS:
                    return Http400("Invalid status value %r" % nodeStatus)
                else:
                    if node.setStatus(nodeStatus):
                        self.writeCallback("Status set to %r" % nodeStatus)
                    else:
                        self.writeCallback("Status was not changed.")


class NodePausedResource(NodesResource):
    @queue
    def put(self, nodeId):
        data = self.getBodyAsJSON()
        try:
            paused = data['paused']
        except KeyError:
            return Http400('Missing entry: "paused".')
        else:
            nodeId = int(nodeId)
            node = self._findNode(nodeId)
            node.setPaused(paused)
        self.writeCallback("Paused flag changed.")


class NodePauseKillResource(NodesResource):
    @queue
    def put(self, nodeId):
        nodeId = int(nodeId)
        node = self._findNode(nodeId)
        if hasattr(node, "taskGroup"):
            for task in node.taskGroup.tasks:
                self.pauseandkill(task)
        else:
            self.pauseandkill(node.task)
        for poolShare in node.poolShares:
            poolShare.allocatedRN = 0
        node.setPaused(True)

    def pauseandkill(self, task):
        for command in task.commands:
            if command.status is CMD_RUNNING:
                command.setReadyAndKill()


class NodePriorityResource(NodesResource):
    @queue
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
    @queue
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
    @queue
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
    @queue
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
            return Http400("Failed to set strategy: %s" % e)
        else:
            node.strategy = strategyClass()
            message = "Strategy for node %d set to %s." % (node.id, repr(strategyClassName))
        self.writeCallback(message)


class NodeChildrenResource(NodesResource):
    @queue
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
                    return Http404("Node not found", "Node %d not found." % nodeId, "text/plain")
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
        body = json.dumps(odict)
        self.set_status(200)
        self.writeCallback(body)
