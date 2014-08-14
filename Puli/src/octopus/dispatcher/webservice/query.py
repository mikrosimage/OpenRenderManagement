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

    'items': 
        [
            {
                attr1: data,
                attr2: data,
                attr3: data
                'items':
                    [
                        {
                            attr1: data,
                            attr2: data,
                            attr3: data
                            'items': [...]
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

from tornado.web import HTTPError

from octopus.dispatcher.model import FolderNode
from octopus.dispatcher.model.nodequery import IQueryNode

from octopus.core.communication.http import Http404, Http400, Http500, HttpConflict
from octopus.core.framework import queue
from octopus.dispatcher.webservice import DispatcherBaseResource

__all__ = []

logger = logging.getLogger('query')

class QueryResource(DispatcherBaseResource, IQueryNode):
    ADDITIONNAL_SUPPORTED_FIELDS = ['pool', 'userDefinedMaxRn']
    DEFAULT_FIELDS = ['id','user','name', 'tags:prod', 'tags:shot', \
                     'status', 'completion', 'dispatchKey', \
                     'startTime', 'creationTime', 'endTime', 'updateTime', \
                     'averageTimeByFrame', 'maxTimeByFrame', 'minTimeByFrame', \
                     'maxRN', 'allocatedRN', 'maxAttempt']


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
            #
            # Get value of additionnally supported field
            #
            if currArg.startswith("tags:"):
                # Attribute name references a "tags" item
                tag = unicode(currArg[5:])
                value = unicode(pNode.tags.get(tag,''))
                currTask[tag] = value
            elif currArg == "pool":
                # Attribute 'pool' is a specific item
                currTask[currArg] = pNode.poolShares.keys()[0].name
            elif currArg == "userDefinedMaxRn":
                # Attribute 'userDefiniedMaxRN' is a specific item
                currTask[currArg] = pNode.poolShares.values()[0].userDefinedMaxRN

            #
            # Get value of standard field
            #
            else:
                # Attribute is a standard attribute of a Node
                currTask[currArg] =  getattr(pNode, currArg, 'undefined')

        if pTree and hasattr(pNode, 'children'):
            childTasks = []
            for child in pNode.children:
                childTasks.append( self.createTaskRepr( child, pAttributes, pTree ) )

            currTask['items'] = childTasks
        return currTask

    def get(self):
        """
        Handle user query request.
          1. init timer and result struct
          2. check attributes to retrieve
          3. limit nodes list regarding the given query filters
          4. for each filtered node: add info in result
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
                            'items':resultData }
            
                self.writeCallback( json.dumps(content) )
                return


            #
            # --- Check if display attributes are valid
            #     We handle 2 types of attributes: 
            #       - simple node attributes
            #       - "tags" node attributes (no verification, it is not mandatory)
            #
            # if 'attr' in args:
            #     for currAttribute in args['attr']:
            #         if not currAttribute.startswith("tags:"):
            #             if not hasattr(nodes[0],currAttribute):
            #                 if currAttribute not in QueryResource.ADDITIONNAL_SUPPORTED_FIELDS :
            #                     logger.warning('Error retrieving data, invalid attribute requested : %s', currAttribute )
            #                     raise HTTPError( 500, "Invalid attribute requested: %s" % (currAttribute) )
            # else:
            #     # Using default result attributes
            #     args['attr'] = QueryResource.DEFAULT_FIELDS
            if 'attr' not in args:
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
                        'items':resultData 
                        }

            # Create response and callback
            self.writeCallback( json.dumps(content) )


        except KeyError:
            raise Http404('Error unknown key')
        
        except HTTPError, e:
            raise e

        except Exception, e:
            logger.warning('Impossible to retrieve result for query: %s', self.request.uri)
            raise HTTPError( 500, "Internal error")





class RenderNodeQueryResource(DispatcherBaseResource, IQueryNode):
    """
    id: 3,
    name: "vfxpc64:9002",
    host: "vfxpc64",
    port: 9002,

    ramSize: 3959,
    coresNumber: 8,
    speed: 2.66,
    performance: 0,

    status: 3,
    lastAliveTime: 1384251282.599067,
    pools: ["renderfarm"],

    caracteristics: {
            distribname: "openSUSE 12.1",
            mikdistrib: "2.3.3",
            cpuname: "Intel(R) Xeon(R) CPU E5430 @ 2.66GHz",
            openglversion: "3.3.0",
            os: "linux",
            softs: [ ]
        },

    isRegistered: true,
    excluded: false,
    commands: [ ],

    usedRam: [ ],
    usedCoresNumber: [ ],
    freeCoresNumber: 8,
    freeRam: 3959
    """


    ADDITIONNAL_SUPPORTED_FIELDS = ['caracteristics:mikdistrib', 'caracteristics:distribname', 'caracteristics:openglversion']
    DEFAULT_FIELDS = ['id', 'name', 'host', 'port', 'ramSize', 'coresNumber', 'speed', 'status', 'lastAliveTime', 'createDate', 'registerDate', 'puliversion', 'pools', 'systemFreeRam', 'systemSwapPercentage' ]



    def createRepr( self, pRenderNode, pAttributes ):
        """
        Create a json representation for a given node.
        param: rendernode to represent
        param: attributes to retrieve on each node
        return: a json dict
        """
        result = {}
        for currArg in pAttributes:
            #
            # Get value of additionnally supported field
            #
            if currArg.startswith("caracteristics:"):
                # Attribute name references a "tags" item
                caract = unicode(currArg[15:])
                value = unicode(pRenderNode.caracteristics.get(caract,''))
                result[caract] = value
            elif currArg == "pools":
                # Attribute name is a specific item
                result[currArg] = [ pool.name for pool in pRenderNode.pools ]
                # result[currArg] = ",".join( str(pRenderNode.pools))

            else:
                # Attribute is a standard attribute of a Node
                result[currArg] =  getattr(pRenderNode, currArg, 'undefined')

        return result

    def get(self):
        """
        Handle user query request.
          1. init timer and result struct
          2. check attributes to retrieve
          3. limit nodes list regarding the given query filters
          4. for each filtered node: add info in result
        """
        args = self.request.arguments

        try:
            start_time = time.time()
            resultData = []
            filteredRN = []

            rn = self.getDispatchTree().renderNodes.values()
            totalNodes = len(rn)

            #
            # --- Check if display attributes are valid
            #     We handle 2 types of attributes: 
            #       - simple node attributes
            #       - "tags" node attributes (no verification, it is not mandatory)
            #
            if 'attr' in args:
                for currAttribute in args['attr']:
                    if not hasattr(rn[0],currAttribute):
                        if currAttribute not in RenderNodeQueryResource.ADDITIONNAL_SUPPORTED_FIELDS :
                            logger.warning('Error retrieving data, invalid attribute requested : %s', currAttribute )
                            raise HTTPError( 500, "Invalid attribute requested:"+str(currAttribute) )
            else:
                # Using default result attributes
                args['attr'] = RenderNodeQueryResource.DEFAULT_FIELDS


            #
            # --- filtering
            #
            filteredRN = self.filterRenderNodes( args, rn )


            #
            # --- Prepare the result json object
            #
            for currNode in filteredRN:
                currItem = self.createRepr( currNode, args['attr'] )
                resultData.append( currItem )

            content = { 
                        'summary': 
                            { 
                            'count':len(filteredRN), 
                            'totalInDispatcher':totalNodes, 
                            'requestTime':time.time() - start_time,
                            'requestDate':time.ctime()
                            }, 
                        'items':resultData 
                        }

            #
            # --- Create response and callback
            #
            self.writeCallback( json.dumps(content) )

        except Exception, e:
            logger.warning('Impossible to retrieve query result for rendernodes: %s', self.request.uri)
            raise HTTPError( 500, "Internal error")
        pass
