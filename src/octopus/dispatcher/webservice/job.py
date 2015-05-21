# -*- coding: utf8 -*-
from __future__ import absolute_import

"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2014, Mikros Image"

import logging
import time
try:
    import simplejson as json
except ImportError:
    import json

from tornado.web import HTTPError

from octopus.core.communication.http import Http404
from octopus.core.framework import ResourceNotFoundError
from octopus.dispatcher.webservice import DispatcherBaseResource
from octopus.dispatcher.model.filter.node import IFilterNode
from octopus.dispatcher.model import Task as DispatcherTask
from puliclient.model.job import Job
from puliclient.model.task import Task

class JobNotFoundError(ResourceNotFoundError):
    '''
    Raised when a request is sent for a node that is not a attached to root.
    '''
    def __init__(self, node, *args, **kwargs):
        ResourceNotFoundError.__init__(self, node=node, *args, **kwargs)


class JobQueryResource(DispatcherBaseResource, IFilterNode):

    def createJobRepr(self, pNode, recursive=True):
        """
        Create a json representation for a given node hierarchy.
        param: node to explore
        return: puliclient.model.job object (which is serializable)
        """
        newJob = Job()
        newJob.createFromNode(pNode)

        if not recursive:
            return newJob
        else:
            if hasattr(pNode, 'children'):
                for node in pNode.children:
                    newJob.children.append(self.createJobRepr(node))

            if hasattr(pNode, 'task') and isinstance(pNode.task, DispatcherTask):
                newJob.task = Task()
                newJob.task.createFromTaskNode(pNode.task)

        return newJob

    def post(self):
        """
        """
        self.logger = logging.getLogger('main.query')

        filters = self.getBodyAsJSON()
        self.logger.debug('filters: %s' % filters)

        try:
            start_time = time.time()
            resultData = []

            # Root node is node 1.
            nodes = self.getDispatchTree().nodes[1].children
            totalNodes = len(nodes)

            #
            # --- filtering
            #
            filteredNodes = self.matchNodes(filters, nodes)
            # self.logger.debug("Nodes have been filtered")

            #
            # --- Prepare the result json object
            #
            for currNode in filteredNodes:
                tmp = self.createJobRepr(currNode, filters.get('recursive', True))
                resultData.append(tmp.encode())

            content = {
                'summary': {
                    'count': len(filteredNodes),
                    'totalInDispatcher': totalNodes,
                    'requestTime': time.time() - start_time,
                    'requestDate': time.ctime()
                },
                'items': resultData
            }

            # Create response and callback
            self.writeCallback(json.dumps(content))

        except KeyError:
            raise Http404('Error unknown key')

        except HTTPError, e:
            raise e

        except Exception, e:
            raise HTTPError(500, "Impossible to retrieve jobs (%s)" % e)
