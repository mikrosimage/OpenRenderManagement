#!/usr/bin/python
# -*- coding: latin-1 -*-

'''
Note les requetes http types présentent les arguments de la manière suivante:
field1=value1&field2=value2&field3=value3, Tonado autorise la définition de plusieurs valeurs pour un field donné

Le webservice prend en charge les requêtes de la forme:
edit?update_status=0&constraint_user=jsa
edit?update_status=1&constraint_user=jsa&constraint_user=render
edit?update_status=0&constraint_id=1&constraint_id=2&constraint_id=3

On retourne un objet json au format:
{
    'summary':
    {
        'count': int,
        'totalInDispatcher': int,
        'requestTime': datetime,
        'requestDate': datetime,
    }
}

Inspire du comportement d'outil comme condor_q/condor_status
'''

try:
    import simplejson as json
except ImportError:
    import json

import logging
import time
from datetime import datetime

from octopus.dispatcher.model import FolderNode
from octopus.dispatcher.model.enums import NODE_STATUS
from octopus.dispatcher.model.nodequery import IQueryNode

from octopus.core.enums.node import NODE_ERROR, NODE_CANCELED, NODE_DONE, NODE_READY
from octopus.core.communication.http import Http404, Http400, Http500, HttpConflict

from octopus.core.framework import BaseResource, queue
from octopus.dispatcher.webservice import DispatcherBaseResource

__all__ = []

logger = logging.getLogger('main.dispatcher.webservice.editController')


class EditStatusResource(DispatcherBaseResource, IQueryNode):
    """
    Handle user request to change the status on a set of nodes. Status value will be change according to transition constraints
    in setStatusForNode() class member.
    """

    def getNode(self, nodeId):
        try:
            return self.getDispatchTree().nodes[int(nodeId)]
        except KeyError:
            raise KeyError

    def setStatusForNode(self, pStatus, pNode):
        """
        Changes the status for a particular node. Properly reset completion when restarting a node.
        Returns the node id IF the node was actually edited, None otherwise
        """

        if pNode.status == pStatus:
            logger.info("Status is already %d for node %d" % (pStatus, pNode.id))
            return None

        if pStatus in [NODE_ERROR, NODE_CANCELED, NODE_DONE] and pStatus == NODE_READY:
            logger.info("Reset completion for node %d" % pNode.id)
            pNode.resetCompletion()
        else:
            logger.info("Set new status %d" % pStatus)
            pNode.setStatus(pStatus)

        return pNode.id

    def put(self):
        """

        """
        start_time = time.time()
        prevTimer = time.time()
        editedJobs = []

        nodes = self.getDispatchTree().nodes[1].children
        totalNodes = len(nodes)

        args = self.request.arguments

        if 'update_status' in args:
            newStatus = int(args['update_status'][0])
        else:
            raise Http400('New status could not be found.')

        if newStatus not in NODE_STATUS:
            raise Http400("Invalid status given: %d" % newStatus)

        # # Optional argument to allow job to be restarted (if defined) or only resumed (if nothing defined)
        # if 'update_option' in args:
        #     if args['update_option'][0] == "restart" :
        #         restartNode = True

        nodes = self.filterNodes(args, nodes)

        for currNode in nodes:
            # logger.info("Changing status for job : %d -- %s" % ( currNode.id, currNode.name ) )
            try:
                if self.setStatusForNode(newStatus, currNode) is not None:
                    editedJobs.append(currNode.id)
            except:
                raise Http500('Error changing status.')

        content = {
            'summary': {
                'editedCount': len(editedJobs),
                'filteredCount': len(nodes),
                'totalInDispatcher': totalNodes,
                'requestTime': time.time() - start_time,
                'requestDate': time.ctime()
            },
            'editedJobs': editedJobs
        }

        # Create response and callback
        self.writeCallback(json.dumps(content))


class PauseResource(DispatcherBaseResource, IQueryNode):
    """
    Hanlde user requests to pause a specific set of jobs
    """
    def getNode(self, nodeId):
        try:
            return self.getDispatchTree().nodes[int(nodeId)]
        except KeyError:
            raise KeyError

    def put(self):
        """

        """
        start_time = time.time()
        prevTimer = time.time()
        editedJobs = []

        nodes = self.getDispatchTree().nodes[1].children
        totalNodes = len(nodes)

        args = self.request.arguments

        nodes = self.filterNodes(args, nodes)
        for currNode in nodes:
            try:
                if hasattr(currNode, 'paused') and currNode.paused is False:
                    currNode.setPaused(True)
                    editedJobs.append(currNode.id)
            except:
                raise Http400('Error when pausing job.')

        content = {
            'summary': {
                'editedCount': len(editedJobs),
                'filteredCount': len(nodes),
                'totalInDispatcher': totalNodes,
                'requestTime': time.time() - start_time,
                'requestDate': time.ctime()
            },
            'editedJobs': editedJobs
        }

        # Create response and callback
        self.writeCallback(json.dumps(content))


class ResumeResource(DispatcherBaseResource, IQueryNode):
    """
    Hanlde user requests to resume a specific set of jobs
    """
    def getNode(self, nodeId):
        try:
            return self.getDispatchTree().nodes[int(nodeId)]
        except KeyError:
            raise KeyError

    def put(self):
        """

        """
        start_time = time.time()
        prevTimer = time.time()
        editedJobs = []

        nodes = self.getDispatchTree().nodes[1].children
        totalNodes = len(nodes)

        args = self.request.arguments

        nodes = self.filterNodes(args, nodes)
        for currNode in nodes:
            try:
                # if hasattr(currNode, 'resume') and currNode.paused == True:
                currNode.setPaused(False)
                editedJobs.append(currNode.id)
            except:
                raise Http400('Error when resuming job.')

        content = {
            'summary': {
                'editedCount': len(editedJobs),
                'filteredCount': len(nodes),
                'totalInDispatcher': totalNodes,
                'requestTime': time.time() - start_time,
                'requestDate': time.ctime()
            },
            'editedJobs': editedJobs
        }

        # Create response and callback
        self.writeCallback(json.dumps(content))


class EditMaxRnResource(DispatcherBaseResource, IQueryNode):
    """
    Edit multiple jobs with a query for filtering.
    Value edited is maxRN field which indicates a max number of rendernodes to assign to a specific job.
    maxRN is an integer > -1

    test: curl -X PUT  http://pulitest:8004/edit/maxrn?value=2&constraint_id=5028
    """

    def put(self):
        """
        """
        start_time = time.time()
        prevTimer = time.time()
        editedJobs = []

        nodes = self.getDispatchTree().nodes[1].children
        totalNodes = len(nodes)

        #
        # Getting new value, we check its presence in args, type (int) and range (>-1)
        #
        try:
            newMaxRn = int(self.get_argument('value'))  # Tornado raises a MissingArgumentError if not present
        except ValueError:
            raise Http400('Bad request:  invalid value, maxRN must be an integer')

        if newMaxRn < -1:
            raise Http400('Bad request: invalid value, maxRN cannot be lower than -1')

        #
        # Filtering nodes
        #
        nodes = self.filterNodes(self.request.arguments, nodes)

        #
        # Perform action
        #
        for currNode in nodes:
            try:
                currNode.maxRN = newMaxRn
                editedJobs.append(currNode.id)
            except:
                raise Http500('Error changing status of job: %d.', currNode.id)

        #
        # Prepare response and return
        #
        content = {
            'summary':  {
                'editedCount': len(editedJobs),
                'filteredCount': len(nodes),
                'totalInDispatcher': totalNodes,
                'requestTime': time.time() - start_time,
                'requestDate': time.ctime()
            },
            'editedJobs': editedJobs
        }
        self.writeCallback(json.dumps(content))



class EditPrioResource(DispatcherBaseResource, IQueryNode):
    """
    Edit multiple jobs with a query for filtering.
    Value given is a new priority, it is stored as the dispatchKey which the true field used for job assignation

    test: curl -X PUT  http://pulitest:8004/edit/prio?value=20&contraint_status=1
    """

    def put(self):
        """
        """
        start_time = time.time()
        prevTimer = time.time()
        editedJobs = []

        nodes = self.getDispatchTree().nodes[1].children
        totalNodes = len(nodes)

        #
        # Getting new value, we check its presence in args and type (int)
        #
        try:
            newPrio = int(self.get_argument('value'))  # Tornado raises a MissingArgumentError if not present
        except ValueError:
            raise Http400('Bad request:  invalid value, prio must be an integer')

        #
        # Filtering nodes
        #
        nodes = self.filterNodes(self.request.arguments, nodes)

        for currNode in nodes:
            try:
                currNode.dispatchKey = newPrio
                editedJobs.append(currNode.id)
            except:
                raise Http500('Error changing status of job: %d.', currNode.id)

        #
        # Prepare response and return
        #
        content = {
            'summary': {
                'editedCount': len(editedJobs),
                'filteredCount': len(nodes),
                'totalInDispatcher': totalNodes,
                'requestTime': time.time() - start_time,
                'requestDate': time.ctime()
            },
            'editedJobs': editedJobs
        }

        self.writeCallback(json.dumps(content))


class RenderNodeEditResource(DispatcherBaseResource, IQueryNode):
    def get(self):
        """
        """
        args = self.request.arguments
        logger.info("args: %r" % args)
        pass
