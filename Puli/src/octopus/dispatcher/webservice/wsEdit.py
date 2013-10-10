#!/usr/bin/python
# -*- coding: latin-1 -*-

'''
Note les requetes http types présentent les arguments de la manière suivante:
field1=value1&field2=value2&field3=value3, Tonado autorise la définition de plusieurs valeurs pour un field donné

Le webservice prend en charge les requêtes de la forme:
edit?value=0&constraint_user=jsa
edit?value=1&constraint_user=jsa&constraint_user=render
edit?value=0&constraint_id=1&constraint_id=2&constraint_id=3

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

__all__ = []

logger = logging.getLogger('dispatcher.webservice.wsEditController')

class EditResource(BaseResource, IQueryNode):

    def getNode(self, nodeId):
        try:
            return self.getDispatchTree().nodes[int(nodeId)]
        except KeyError:
            raise KeyError


    def setStatusForNode( self, pStatus, pNode ):
        """
        Changes the status for a particular node. Properly reset completion when restarting a node.
        Returns the node id IF the node was actually edited, None otherwise
        """

        if pNode.status == pStatus:
            logger.info("Status is already %d for node %d" % (pStatus, pNode.id))
            return None

        if pStatus in [NODE_ERROR, NODE_CANCELED, NODE_DONE] and pStatus == NODE_READY:
            pNode.resetCompletion()
        else:
            pNode.setStatus(pStatus)

        return pNode.id



    def put(self):
        """

        """
        start_time = time.time()
        prevTimer = time.time()

        nodes = self.getDispatchTree().nodes[1].children
        totalNodes = len(nodes)

        editedJobs = []

        args = self.request.arguments

        if 'update_status' in args:
            newStatus = int(args['update_status'][0])
        else:
            return Http400('New status could not be found.')

        if newStatus not in NODE_STATUS:
            return Http400("Invalid status given: %d" % newStatus)


        nodes = self.filterNodes( args, nodes )
        # logger.info("%.2f ms - Nodes filtered (%d of %d)" % ( (time.time() - prevTimer)*1000, len(nodes), totalNodes ) )
        # prevTimer = time.time()

        for currNode in nodes:
            # logger.info("Changing status for job : %d -- %s" % ( currNode.id, currNode.name ) )
            try:
                if self.setStatusForNode( newStatus, currNode ) is not None:
                    editedJobs.append( currNode.id )
            except:
                return Http400('Error changing status.')


        content = { 
                    'summary': 
                        { 
                        'editedCount':len(editedJobs),
                        'filteredCount':len(nodes),
                        'totalInDispatcher':totalNodes, 
                        'requestTime':time.time() - start_time,
                        'requestDate':time.ctime()
                        }, 
                    'editedJobs':editedJobs 
                    }

        # Create response and callback
        self.writeCallback( json.dumps(content) )
