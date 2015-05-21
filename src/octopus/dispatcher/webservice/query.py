#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Un webservice permettant de pouvoir repondre a des requetes de la sorte:

Note les requetes http types presentent les arguments de la maniere suivante:
field1=value1&field2=value2&field3=value3, Tonado autorise la definition de plusieurs valeurs pour un field donne

Le webservice prend en charge les requetes de la forme:
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
"""

try:
    import simplejson as json
except ImportError:
    import json

import logging
import time
import types
import re

from tornado.web import HTTPError

from octopus.dispatcher.model.nodequery import IQueryNode

from octopus.core.communication.http import Http404, Http400, Http500, HttpConflict
from octopus.dispatcher.webservice import DispatcherBaseResource


__all__ = []

logger = logging.getLogger('main.query')


class QueryResource(DispatcherBaseResource, IQueryNode):
    ADDITIONNAL_SUPPORTED_FIELDS = ['pool', 'userDefinedMaxRn']
    DEFAULT_FIELDS = ['id', 'user', 'name', 'tags:prod', 'tags:shot',
                      'status', 'completion', 'dispatchKey',
                      'startTime', 'creationTime', 'endTime', 'updateTime',
                      'averageTimeByFrame', 'maxTimeByFrame', 'minTimeByFrame',
                      'maxRN', 'optimalMaxRN', 'allocatedRN', 'maxAttempt',
                      'commandCount', 'readyCommandCount', 'doneCommandCount']

    def createTaskRepr(self, pNode, pAttributes, pTree=False):
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
            try:
                if currArg.startswith("tags:"):
                    # Attribute name references a "tags" item
                    currArg = unicode(currArg[5:])
                    value = unicode(pNode.tags.get(currArg, ''))
                    currTask[currArg] = value
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
                    currTask[currArg] = getattr(pNode, currArg, 'undefined')

            except AttributeError:
                currTask[currArg] = 'undefined'
                logger.warning("Impossible to get attribute '%s' on object %r" % (currArg, pNode))

        if pTree and hasattr(pNode, 'children'):
            childTasks = []
            for child in pNode.children:
                childTasks.append(self.createTaskRepr(child, pAttributes, pTree))

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
            tree = False

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
                content = {
                    'summary': {
                        'count': 0,
                        'totalInDispatcher': 0,
                        'requestTime': time.time() - start_time,
                        'requestDate': time.ctime()
                    },
                    'items': resultData
                }

                self.writeCallback(json.dumps(content))
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
            filteredNodes = self.filterNodes(args, nodes)

            #
            # --- Prepare the result json object
            #
            for currNode in filteredNodes:
                currTask = self.createTaskRepr(currNode, args['attr'], tree)
                resultData.append(currTask)

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
            logger.warning('Impossible to retrieve result for query: %s', self.request.uri)
            raise HTTPError(500, "Internal error")



    # def createJobRepr(self, pNode, recursive=True):
    #     """
    #     Create a json representation for a given node hierarchy.
    #     param: node to explore
    #     return: puliclient.model.job object (which is serializable)
    #     """
    #
    #
    #     newJob = Job()
    #     newJob.createFromNode(pNode)
    #
    #     if not recursive:
    #         return newJob
    #     else:
    #         if hasattr(pNode, 'children'):
    #             for node in pNode.children:
    #                 newJob.children.append(self.createJobRepr(node))
    #
    #         if hasattr(pNode, 'task') and isinstance(pNode.task, DispatcherTask):
    #             newJob.task = Task()
    #             newJob.task.createFromTaskNode(pNode.task)
    #
    #     return newJob
    #
    # def post(self):
    #     """
    #     """
    #     self.logger = logging.getLogger('main.query')
    #
    #     filters = self.getBodyAsJSON()
    #     self.logger.debug('filters: %s' % filters)
    #
    #     try:
    #         start_time = time.time()
    #         resultData = []
    #
    #         nodes = self.getDispatchTree().nodes[1].children
    #         totalNodes = len(nodes)
    #         # self.logger.debug("All nodes retrieved")
    #         #
    #         # --- filtering
    #         #
    #         filteredNodes = self.matchNodes(filters, nodes)
    #         # self.logger.debug("Nodes have been filtered")
    #
    #         #
    #         # --- Prepare the result json object
    #         #
    #         for currNode in filteredNodes:
    #             tmp = self.createJobRepr(currNode, filters.get('recursive', True))
    #             resultData.append(tmp.encode())
    #         # self.logger.debug("Representation has been created")
    #
    #         content = {
    #             'summary': {
    #                 'count': len(filteredNodes),
    #                 'totalInDispatcher': totalNodes,
    #                 'requestTime': time.time() - start_time,
    #                 'requestDate': time.ctime()
    #             },
    #             'items': resultData
    #         }
    #
    #         # Create response and callback
    #         self.writeCallback(json.dumps(content))
    #         # self.logger.debug("Result sent")
    #
    #     except KeyError:
    #         raise Http404('Error unknown key')
    #
    #     except HTTPError, e:
    #         raise e
    #
    #     except Exception, e:
    #         raise HTTPError(500, "Impossible to retrieve jobs (%s)" % e)


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
    DEFAULT_FIELDS = ['id', 'name', 'host', 'port', 'ramSize', 'coresNumber', 'speed', 'status', 'lastAliveTime', 'createDate', 'registerDate', 'puliversion', 'pools', 'systemFreeRam', 'systemSwapPercentage']

    def createRepr(self, pRenderNode, pAttributes):
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
                value = unicode(pRenderNode.caracteristics.get(caract, ''))
                result[caract] = value
            elif currArg == "pools":
                # Attribute name is a specific item
                result[currArg] = [pool.name for pool in pRenderNode.pools]
                # result[currArg] = ",".join( str(pRenderNode.pools))

            else:
                # Attribute is a standard attribute of a Node
                result[currArg] = getattr(pRenderNode, currArg, 'undefined')

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
                    if not hasattr(rn[0], currAttribute):
                        if currAttribute not in RenderNodeQueryResource.ADDITIONNAL_SUPPORTED_FIELDS:
                            logger.warning('Error retrieving data, invalid attribute requested : %s', currAttribute)
                            raise HTTPError(500, "Invalid attribute requested:" + str(currAttribute))
            else:
                # Using default result attributes
                args['attr'] = RenderNodeQueryResource.DEFAULT_FIELDS

            #
            # --- filtering
            #
            filteredRN = self.filterRenderNodes(args, rn)

            #
            # --- Prepare the result json object
            #
            for currNode in filteredRN:
                currItem = self.createRepr(currNode, args['attr'])
                resultData.append(currItem)

            content = {
                'summary': {
                    'count': len(filteredRN),
                    'totalInDispatcher': totalNodes,
                    'requestTime': time.time() - start_time,
                    'requestDate': time.ctime()
                },
                'items': resultData
            }

            #
            # --- Create response and callback
            #
            self.writeCallback(json.dumps(content))

        except Exception:
            logger.warning('Impossible to retrieve query result for rendernodes: %s', self.request.uri)
            raise HTTPError(500, "Internal error")
        pass

##################################
##################################
##################################
##################################
##################################
##################################
##################################
##################################

class RenderNodeQuery2Resource(DispatcherBaseResource):
    """
    """

    def strMatch(self, field, condition):
        return True if re.match(condition, field) else False

    def intMatch(self, field, value):
        raise NotImplementedError
        pass

    def matchField(self, field, fieldName, condition):
        self.logger.debug("type: %s" % type(field))
        currType = type(field)

        if currType == types.StringType:
            return self.strMatch(field, condition)
        elif currType == types.IntType:
            return self.intMatch(field, condition)

        # return True if item.__dict__[field] == condition else False
        # return True if item.__dict__[field] == value else False

    def matchQuery(self, item):
        """
        Parses a given query against the item given.
        Returns True or False
        :param item:
        :return:
        """
        self.logger.debug(item)
        if len(self.queryDict) > 1:
            self.logger.debug("Invalid query dict")
            return False

        if ('or' in self.queryDict) or ('and' in self.queryDict):
            self.logger.debug("or/and")
            # TODO

        else:
            self.fieldName = self.queryDict.keys()[0]
            self.condition = self.queryDict[self.fieldName]
            self.currObjField = getattr(item, self.fieldName)

            self.logger.debug("Request on single field: %s" % self.fieldName)

            if ('or' in self.condition):
                self.logger.debug("multivalue expression OR: %s" % self.condition)
                # TODO

            elif 'and' in self.condition:
                self.logger.debug("multivalue expression AND: %s" % self.condition)
                # TODO

            elif 'match' in self.condition:
                self.logger.debug("value expression: %s = %s" % (self.currObjField, self.condition))
                res = self.matchField(self.currObjField, self.fieldName, self.condition['match'])
                self.logger.debug('match = %s' % res)
                return res
            else:
                self.logger.debug("invalid expression: %s" % self.value)

        return False

    def post(self):
        """
        Handle user query request.
          1. init timer and result struct
          2. check attributes to retrieve
          3. limit nodes list regarding the given query filters
          4. for each filtered node: add info in result
        """
        self.logger = logging.getLogger('main.query2')

        # Default response
        resultData = []
        response = {
            'summary': {
                'count': 0,
                'totalInDispatcher': 0,
                'requestTime': 0,
                'requestDate': 0
            },
            'items': resultData
        }

        try:
            # Get query in body
            try:
                self.queryDict = json.loads(self.request.body)
            except Exception as e:
                self.logger.error(e)

            # Find corresponding rn
            matches = filter(self.matchQuery, self.getDispatchTree().renderNodes.values())

            # Prepare json result
            resultData = [n.to_json() for n in matches]

            lenTotalData = len(self.getDispatchTree().renderNodes)

            # Update default response
            response['summary']['count'] = len(resultData)
            response['summary']['totalInDispatcher'] = lenTotalData
            response['summary']['requestExecutionTime'] = time.time() - self.startTime
            response['summary']['requestDate'] = time.ctime()
            response['items'] = resultData

            self.write(response)
        except Exception as e:
            self.logger.error(e)
            raise HTTPError(500, "Impossible to retrieve rendernodes for this request")
