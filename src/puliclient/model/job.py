#!/usr/bin/python
# -*- coding: utf8 -*-
from __future__ import absolute_import

"""
"""
__author__ = "Jerome Samson"
__copyright__ = "Copyright 2015, Mikros Image"

try:
    import simplejson as json
except ImportError:
    import json

from puliclient.model.jsonModel import JsonModel
from puliclient.model.task import Task
from octopus.dispatcher.model import Task as DispatcherTask

class Job(object, JsonModel):
    #
    # Private
    #
    def __init__(self, jobDict=None):
        # Core infos
        self.id = 0
        self.name = ""
        self.user = ""
        self.status = 0
        self.creationTime = 0
        self.updateTime = 0
        self.startTime = 0
        self.endTime = 0

        # Additionnal infos
        self.tags = {}
        self.commandCount = 0
        self.doneCommandCount = 0
        self.readyCommandCount = 0

        # Assignment
        self.dispatchKey = 0
        self.maxRN = 0
        self.timer = None

        # Progress and stats
        self.completion = 0.0
        self.averageTimeByFrame = 0
        self.minTimeByFrame = 0
        self.maxTimeByFrame = 0

        self.dependencies = []

        # Internal infos
        self.task = None
        self.children = []
        self.poolShares = []
        self.additionnalPoolShares = []
        #updateTime = models.FloatField(allow_null=True)

        if jobDict:
            self._createFromDict(jobDict)


    def __repr__(self):
        return "Job(%s)" % self.name

    def __str__(self):
        return "Job: %d - %s" % (self.id, self.name)

    def encode(self):
        res = {}
        for field in self.__dict__:
            if field == 'children':
                res['children'] = []
                for child in self.children:
                    res['children'].append(child.encode())
            elif field == 'task':
                if self.task:
                    res['task'] = self.task.encode()
            else:
                res[field] = getattr(self, field)
        return res

    def decode(self):
        pass

    def _createFromDict(self, jobDict):
        for key, val in jobDict.iteritems():
            if key == 'children':
                for child in jobDict['children']:
                    self.children.append(Job(child))
            elif key == 'task':
                self.task = Task(jobDict['task'])
                pass
            elif hasattr(self, key):
                setattr(self, key, val)

    def createFromNode(self, node):
        # Core infos
        self.id = node.id
        self.name = node.name
        self.user = node.user
        self.status = node.status
        self.creationTime = node.creationTime
        self.updateTime = node.updateTime
        self.startTime = node.startTime
        self.endTime = node.endTime

        # Additionnal infos
        self.tags = node.tags.copy()
        self.commandCount = node.commandCount
        self.doneCommandCount = node.doneCommandCount
        self.readyCommandCount = node.readyCommandCount

        # Assignment
        self.dispatchKey = node.dispatchKey
        self.maxRN = node.maxRN
        self.timer = node.timer

        # Progress and stats
        self.completion = node.completion
        self.averageTimeByFrame = node.averageTimeByFrame
        self.minTimeByFrame = node.minTimeByFrame
        self.maxTimeByFrame = node.maxTimeByFrame

        # self.dependencies = node.dependencies.copy()

        # Internal infos
        self.task = None
        self.children = []
        # self.poolShares = []
        # self.additionnalPoolShares = []

        # if hasattr(node, 'children'):
        #     for child in node.children:
        #         tmp = Job()
        #         self.children.append(tmp.createFromNode(child))
        #
        # if hasattr(node, 'task') and isinstance(node.task, DispatcherTask):
        #     self.task = Task()
        #     self.task.createFromTaskNode(node.task)


    def _refresh(self):
        url = "/nodes/%d/" % (self.id)

        # try:
        #     dataDict = Server.get(url)
        #     for key, val in dataDict.iteritems():
        #         if hasattr(self, key):
        #             setattr(self, key, val)
        # except (RequestTimeoutError, RequestError):
        #     logging.error("Impossible to refresh job with query: %s" % url)

    def setDispatchKey(self, prio):
        '''
        | Updates dispatchKey (i.e. prio) of a particular node to the server
        | Internal data is updated on succeed to reflect server change
        :param prio: Integer
        :return: A boolean indicating success or failure
        '''
        url = "/nodes/%d/dispatchKey/" % self.id
        body = json.dumps({'dispatchKey': prio})
        # try:
        #     Server.put(url, data=body)
        # except (RequestTimeoutError, RequestError):
        #     logging.error("Impossible to update prio with url %s and content: \
        #         %s" % (url, body))
        #     return False

        # Update internal value (or refresh)
        self.dispatchKey = prio
        return True

    def setPool(self, pool):
        '''
        | Updates pool name of a particular node
        | Internal data is updated on succeed to reflect server change
        :param pool: String representing a pool name
        :return: A boolean indicating success or failure
        '''
        url = "/poolshares/"
        body = json.dumps({'poolName': pool, 'nodeId': self.id, 'maxRN': -1})
        # try:
        #     Server.post(url, data=body)
        # except (RequestTimeoutError, RequestError):
        #     logging.error("Impossible to update data with url %s and content: \
        #         %s" % (url, body))
        #     return False
        return True

    def setMaxRn(self, maxRn):
        '''
        | Updates maxRn of a particular node i.e. the number of RN to affect
        | to this node. Internal data is updated on succeed to reflect server
        | change.
        :param maxRn: Integer
        :return: A boolean indicating success or failure
        '''
        url = "/nodes/%d/maxRN/" % self.id
        body = json.dumps({'maxRN': maxRn})
        # try:
        #     Server.put(url, data=body)
        # except (RequestTimeoutError, RequestError):
        #     logging.error("Impossible to update data with url %s and content: \
        #         %s" % (url, body))
        #     return False

        # Update internal value (or refresh)
        self.maxRN = maxRn
        return True

    def setProd(self, prod):
        pass

    def setShot(self, shot):
        pass

    def setTags(self, tags):
        pass

    def updateTags(self, tags):
        pass