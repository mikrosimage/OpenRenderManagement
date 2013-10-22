#!/usr/bin/python
# -*- coding: latin-1 -*-

'''
Un webservice permettant de pouvoir repondre à des requetes de la sorte:

Note les requetes http types présentent les arguments de la manière suivante:
field1=value1&field2=value2&field3=value3, Tonado autorise la définition de plusieurs valeurs pour un field donné

Le webservice prend en charge les requêtes de la forme:
http://localhost:8004/query?attr=id
http://localhost:8004/query?constraint_user=jsa
http://localhost:8004/query?attr=id&attr=name&attr=user&constraint_user=jsa&constraint_prod=ddd

Les champs sur lesquels peuvent porter les requetes: user,prod,date

On retourne un objet json au format:
{ 
    'summary':
    {
        'count': int,
        'totalInDispatcher': int, 
        'requestTime': datetime,
        'requestDate': datetime,
    } 

    'tasks': 
        [
            {
                attr1: data,
                attr2: data,
                attr3: data
                'tasks':
                    [
                        {
                            attr1: data,
                            attr2: data,
                            attr3: data
                            'tasks': [...]
                        },
                        ...
                    ]

            },
            ...
        ] 
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
from octopus.dispatcher.model.nodequery import IQueryNode

from octopus.core.communication.http import Http404, Http400, Http500, HttpConflict
from octopus.core.framework import BaseResource, queue

__all__ = []

logger = logging.getLogger('dispatcher.webservice.wsQueryController')

class QueryResource(BaseResource, IQueryNode):
    DEFAULT_FIELDS = ['id','user','name', 'tags:prod', 'tags:shot', \
                     'status', 'completion', 'priority', \
                     'startTime', 'creationTime', 'endTime', 'updateTime']


    def createTaskRepr( self, pNode, pAttributes, pTree=False ):
        """
        Create a json representation for a given node hierarchy and user attributes.
        Recursive call to represent the FolderNode/TaskNode tree
        param: node to explore
        param: attributes to retrieve on each node
        param: flag to indicate if user wants to retrieve subtasks (enable recursive call)
        return: a json dict
        """
        currTask = {}
        for currArg in pAttributes:
            if not currArg.startswith("tags:"):
                currTask[currArg] =  getattr(pNode, currArg, 'undefined')
            else:
                tag = unicode(currArg[5:])
                value = unicode(pNode.tags.get(tag,''))
                currTask[tag] = value

        if pTree and hasattr(pNode, 'children'):
            childTasks = []
            for child in pNode.children:
                childTasks.append( self.createTaskRepr( child, pAttributes, pTree ) )

            currTask['tasks'] = childTasks
        return currTask

    # @queue
    def get(self):
        """
        """
        args = self.request.arguments
        if 'tree' in args:
            tree = bool(args['tree'])
        else:
            tree=False

        # import pudb; pu.db
        try:
            start_time = time.time()
            resultData = []
            filteredNodes = []

            nodes = self.getDispatchTree().nodes[1].children
            totalNodes = len(nodes)

            #
            # --- Check if result list (without filtering) is already empty
            #
            if len(nodes) == 0:
                content = { 'summary': { \
                                'count':0, \
                                'totalInDispatcher':0, \
                                'requestTime':time.time() - start_time, \
                                'requestDate':time.ctime() }, \
                            'tasks':resultData }
            
                self.writeCallback( json.dumps(content) )
                return


            #
            # --- Check if display attributes are valid
            #     We handle 2 types of attributes: 
            #       - simple node attributes
            #       - "tags" node attributes (no verification, it is not mandatory)
            #
            if 'attr' in args:
                for currAttribute in args['attr']:
                    if not currAttribute.startswith("tags:"):
                        if not hasattr(nodes[0],currAttribute):
                            logger.warning('Error retrieving data : %s', currAttribute )
                            return Http404("Invalid attribute requested:"+str(currAttribute), "Invalid attribute specified.", "text/plain")
            else:
                # Using default result attributes
                args['attr'] = QueryResource.DEFAULT_FIELDS


            #
            # --- filtering
            #
            filteredNodes = self.filterNodes( args, nodes )


            #
            # --- Prepare the result json object
            #
            for currNode in filteredNodes:
                currTask = self.createTaskRepr(currNode, args['attr'], tree)
                resultData.append( currTask )

            content = { 
                        'summary': 
                            { 
                            'count':len(filteredNodes), 
                            'totalInDispatcher':totalNodes, 
                            'requestTime':time.time() - start_time,
                            'requestDate':time.ctime()
                            }, 
                        'tasks':resultData 
                        }

            # Create response and callback
            self.writeCallback( json.dumps(content) )


        except KeyError:
            return Http404('Error unknown key')
        
        except Exception:
            logger.warning('Impossible to retrieve result for query: %s', self.request.uri)
            return Http500()

