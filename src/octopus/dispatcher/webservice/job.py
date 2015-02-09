# -*- coding: utf8 -*-
from __future__ import absolute_import

"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2014, Mikros Image"

import logging

from octopus.core.framework import ResourceNotFoundError
from octopus.dispatcher.webservice.nodes import NodeNotFoundError

from octopus.dispatcher.webservice import DispatcherBaseResource

from octopus.dispatcher.model import FolderNode
from octopus.dispatcher.model import TaskNode
from octopus.dispatcher.model import Command

from octopus.core.data import JobInfo
from octopus.core.data import TaskInfo
from octopus.core.data import CommandInfo


class JobNotFoundError(ResourceNotFoundError):
    '''
    Raised when a request is sent for a node that is not a attached to root.
    '''
    def __init__(self, node, *args, **kwargs):
        ResourceNotFoundError.__init__(self, node=node, *args, **kwargs)


class JobResource(DispatcherBaseResource):

    def _findJob(self, nodeId):
        node = None
        # import pudb;pu.db
        try:
            node = self.getDispatchTree().nodes[int(nodeId)]
        except KeyError:
            raise NodeNotFoundError(nodeId)

        if node.parent.id != 1:
            raise JobNotFoundError(nodeId)

        return node

    def parseNode(self, node):
        if isinstance(node, FolderNode):
            res = JobInfo(node.id)
            res.loadFrom(node)
            print "Get folder: %s" % node.name
            for child in node.children:
                res.children.append(self.parseNode(child))

        elif isinstance(node, TaskNode):
            print "Get task: %s" % node.name
            res = TaskInfo(node.id)
            res.loadFrom(node)

            for command in node.task.commands:
                res.commands.append(self.parseNode(command))

        elif isinstance(node, Command):
            print "Get command: %s" % node.description
            res = CommandInfo(node.id)
            res.loadFrom(node)

        return res

    def get(self, jobId):

        # import pudb; pu.db

        log = logging.getLogger('main')
        log.info("jobid = %s" % jobId)

        args = self.request.arguments
        log.info("args = %s" % args)

        node = self._findJob(jobId)

        resDict = self.parseNode( node )

        self.write(resDict.to_JSON())

    pass
